/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.bootstrap;

import io.cassandraopensearch.spi.DecommissionContext;
import io.cassandraopensearch.spi.EmbeddedService;
import io.cassandraopensearch.spi.ServiceContext;
import io.cassandraopensearch.spi.ServiceStatus;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * An {@link EmbeddedService} living inside an {@link IsolatedClassLoader}, presented to the
 * supervisor as an ordinary {@code EmbeddedService}.
 *
 * <p>Two things happen on every call, and both matter:
 *
 * <ol>
 *   <li>The thread's context ClassLoader is swapped to the isolated loader for the duration.
 *       Cassandra and OpenSearch both reach for the TCCL — for {@link java.util.ServiceLoader},
 *       for resource lookups, for reflective config loading — and would find the supervisor's
 *       loader, which cannot see their classes.</li>
 *   <li>Exceptions are translated. A failure inside the service is usually an exception class
 *       the supervisor cannot load, and letting it propagate surfaces as an opaque
 *       {@link NoClassDefFoundError}; {@link io.cassandraopensearch.spi.ServiceException#flatten}
 *       rebuilds the cause chain out of types that exist on both sides.</li>
 * </ol>
 *
 * <p>Not thread-safe with respect to lifecycle calls; the supervisor drives those from a single
 * thread. {@link #status()} and {@link #details()} are safe to call concurrently.
 */
public final class IsolatedService implements EmbeddedService, AutoCloseable {

    private final String serviceName;
    private final IsolatedClassLoader loader;
    private final EmbeddedService delegate;

    private IsolatedService(String serviceName, IsolatedClassLoader loader, EmbeddedService delegate) {
        this.serviceName = serviceName;
        this.loader = loader;
        this.delegate = delegate;
    }

    /**
     * Creates the isolated loader and instantiates {@code implementationClass} inside it.
     *
     * <p>The implementation class must be present on {@code classpath} and must not be visible
     * to the supervisor; if it were, the cast below would silently succeed against a
     * supervisor-loaded copy and the isolation would be a no-op. {@link #assertIsolated} checks
     * for exactly that.
     *
     * @param serviceName         short name, e.g. {@code "cassandra"}
     * @param implementationClass fully-qualified name of the {@link EmbeddedService} impl
     * @param classpath           the complete classpath for this service
     */
    public static IsolatedService create(String serviceName, String implementationClass, List<URL> classpath)
            throws Exception {
        IsolatedClassLoader loader = IsolatedClassLoader.forService(serviceName, classpath);
        boolean ok = false;
        try {
            Object instance = callWithContextLoader(loader, () -> {
                Class<?> type = Class.forName(implementationClass, true, loader);
                assertIsolated(serviceName, type, loader);
                return type.getDeclaredConstructor().newInstance();
            });
            IsolatedService service =
                    new IsolatedService(serviceName, loader, (EmbeddedService) instance);
            ok = true;
            return service;
        } finally {
            if (!ok) {
                loader.close();
            }
        }
    }

    /**
     * Fails loudly if the service implementation did not actually end up in the isolated loader.
     *
     * <p>With the default sharing policy this cannot happen: the loader's parent is the platform
     * loader, so the application classpath is unreachable. The check guards the case where a
     * caller supplies a wider {@code sharedClassFilter} and inadvertently routes the service's
     * own packages back to the supervisor — which yields a process that appears to work while
     * providing no isolation at all, and then fails obscurely once the two servers'
     * dependencies collide. Better to refuse to start.
     */
    private static void assertIsolated(String serviceName, Class<?> type, IsolatedClassLoader loader) {
        if (type.getClassLoader() != loader) {
            throw new IllegalStateException(
                    "Service '" + serviceName + "' implementation " + type.getName()
                            + " was loaded by " + type.getClassLoader()
                            + " instead of " + loader
                            + ". Its jar is on the supervisor classpath (lib/boot) when it belongs"
                            + " in lib/" + serviceName + " — the service would run without isolation.");
        }
    }

    // --- EmbeddedService, each call bracketed by a TCCL swap -------------------------------

    @Override
    public String name() {
        return serviceName;
    }

    @Override
    public void start(ServiceContext context) throws Exception {
        runWithContextLoader(loader, () -> delegate.start(context));
    }

    @Override
    public ServiceStatus status() {
        // Must not block and must not throw: the supervisor polls this for health reporting.
        try {
            return callWithContextLoader(loader, delegate::status);
        } catch (Exception e) {
            return ServiceStatus.FAILED;
        }
    }

    @Override
    public Map<String, String> details() {
        try {
            return callWithContextLoader(loader, delegate::details);
        } catch (Exception e) {
            return Map.of("error", String.valueOf(e.getMessage()));
        }
    }

    @Override
    public void prepareDecommission(DecommissionContext context) throws Exception {
        runWithContextLoader(loader, () -> delegate.prepareDecommission(context));
    }

    @Override
    public boolean awaitDecommissionReady(DecommissionContext context) throws Exception {
        return callWithContextLoader(loader, () -> delegate.awaitDecommissionReady(context));
    }

    @Override
    public void abortDecommission(DecommissionContext context) throws Exception {
        runWithContextLoader(loader, () -> delegate.abortDecommission(context));
    }

    @Override
    public void decommission(DecommissionContext context) throws Exception {
        runWithContextLoader(loader, () -> delegate.decommission(context));
    }

    @Override
    public void stop() throws Exception {
        runWithContextLoader(loader, delegate::stop);
    }

    /**
     * Stops the service if it is still running, then discards the isolated loader.
     *
     * <p>After this returns, every class the service loaded becomes eligible for collection —
     * provided no thread it started is still alive, since a running thread pins its loader.
     */
    @Override
    public void close() throws Exception {
        try {
            // stop() unconditionally, relying on the idempotency the SPI requires. Skipping it
            // for terminal states looks like an optimisation but leaks: DECOMMISSIONED and
            // FAILED are both terminal, and both describe a service that still holds an open
            // node — a decommissioned OpenSearch node has left the cluster but its Node object,
            // threads and sockets are all still live, and a service that failed during start
            // may hold whatever it managed to allocate first.
            stop();
        } finally {
            loader.close();
        }
    }

    /** Exposed for tests that assert isolation properties. */
    public IsolatedClassLoader classLoader() {
        return loader;
    }

    // --- TCCL plumbing ---------------------------------------------------------------------

    private interface IsolatedAction {
        void run() throws Exception;
    }

    private static void runWithContextLoader(ClassLoader loader, IsolatedAction action) throws Exception {
        callWithContextLoader(loader, () -> {
            action.run();
            return null;
        });
    }

    private static <T> T callWithContextLoader(ClassLoader loader, Callable<T> action) throws Exception {
        Thread current = Thread.currentThread();
        ClassLoader previous = current.getContextClassLoader();
        current.setContextClassLoader(loader);
        try {
            return action.call();
        } finally {
            current.setContextClassLoader(previous);
        }
    }
}
