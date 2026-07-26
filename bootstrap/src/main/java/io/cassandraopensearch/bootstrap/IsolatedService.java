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
import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

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
 *   <li>Exceptions are translated, on the way out of every call. A failure inside the service is
 *       frequently of a type only the isolated loader can load, and letting one propagate has two
 *       consequences: the supervisor sees an opaque {@link NoClassDefFoundError} instead of the
 *       failure, and anything that retains the exception — a log buffer, a JMX client's stack
 *       trace, the cause {@code DecommissionCoordinator} puts inside a {@code
 *       DecommissionException} and throws across an MBean — retains the isolated loader with it,
 *       and the whole server's worth of classes behind that. So: a {@link ServiceException}
 *       passes through unchanged, its cause chain already being flattened by construction; a
 *       throwable whose own type <i>and</i> whose entire cause chain the supervisor's loader can
 *       resolve passes through unchanged too, because nothing is gained by dressing up an
 *       {@code IllegalStateException}; everything else is wrapped in a {@code ServiceException},
 *       whose constructor runs {@link ServiceException#flatten} to rebuild the cause chain out of
 *       types that exist on both sides.</li>
 * </ol>
 *
 * <p>Not thread-safe with respect to lifecycle calls; the supervisor drives those from a single
 * thread. {@link #status()} and {@link #details()} are safe to call concurrently.
 */
public final class IsolatedService implements EmbeddedService, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IsolatedService.class);

    /** Bound on the cause chain walked when deciding whether a throwable may pass through. */
    private static final int MAX_CAUSE_DEPTH = 100;

    private final String serviceName;
    private final IsolatedClassLoader loader;
    private final EmbeddedService delegate;

    /**
     * The last status the delegate actually returned, and what {@link #status()} falls back to
     * when it cannot obtain a new one.
     *
     * <p>Not a cache: it is the answer to "what do we report when the question cannot be
     * answered". Reporting {@link ServiceStatus#FAILED} there would be a lie with teeth — the
     * supervisor's health check turns {@code FAILED} into a shutdown of the whole JVM, so a
     * failure to read a status would take both servers down with it.
     */
    private volatile ServiceStatus lastObservedStatus = ServiceStatus.NEW;

    /**
     * How many {@link #status()} calls in a row have failed to produce a status — by throwing, or
     * by returning null — reset by the first that produces one.
     *
     * <p>The stale-status fallback above is deliberate — reporting {@code FAILED} because a status
     * could not be read would take the whole JVM down — but on its own it leaves an operator with
     * no way to tell "healthy" from "cannot be asked": a service whose loader is gone reports
     * {@code RUNNING} to JMX and the CLI forever. This is the third signal, surfaced through
     * {@link #details()}, where it costs nothing and no automated decision is taken on it.
     */
    private final AtomicInteger consecutiveStatusFailures = new AtomicInteger();

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

    /**
     * {@inheritDoc}
     *
     * <p>Must not block and must not throw: the supervisor polls this for health reporting, and —
     * the case that bites — {@code Supervisor.stopService} calls it once more <i>after</i>
     * {@link #close()}, outside any {@code try}. A delegate whose loader has just been closed
     * answers a call into a not-yet-loaded class with {@link NoClassDefFoundError}, which is not
     * an {@link Exception}; letting that escape aborts the shutdown before it records the final
     * state, removes the shutdown hook or counts the process down, and the JVM hangs on exit.
     * Hence {@link Throwable}.
     *
     * <p>A {@code null} from the delegate is one of the ways the question goes unanswered, not an
     * answer: the SPI has no null status, the supervisor's own {@code recordFinalStatus} guards
     * against one, and a delegate that has lost track of its state is exactly the case the stale
     * fallback exists for. Counting it as a success reset {@code consecutiveStatusFailures} to zero
     * on every call and then returned the stale value anyway — so a delegate returning null forever
     * reported {@code RUNNING} to JMX and to the CLI with nothing anywhere saying it was stale,
     * which is the one thing {@code status_stale} was added to make impossible.
     *
     * @return the delegate's status, or the last one it managed to report. Never
     *         {@link ServiceStatus#FAILED} merely because the status could not be determined:
     *         {@code FAILED} means "this service has failed", the supervisor shuts the process
     *         down on it, and "we could not ask" is not that. {@link #details()} says which of the
     *         two this was. Never null.
     */
    @Override
    public ServiceStatus status() {
        try {
            ServiceStatus current = callWithContextLoader(loader, delegate::status);
            if (current == null) {
                ServiceStatus last = lastObservedStatus;
                int failures = consecutiveStatusFailures.incrementAndGet();
                LOG.error("Service '{}' reported a null status ({} consecutive failures to read"
                        + " one); reporting the last observed status {} instead",
                        serviceName, failures, last);
                return last;
            }
            lastObservedStatus = current;
            consecutiveStatusFailures.set(0);
            return current;
        } catch (Throwable t) {
            restoreInterruptIfInterrupted(t);
            ServiceStatus last = lastObservedStatus;
            int failures = consecutiveStatusFailures.incrementAndGet();
            LOG.error("Could not read the status of service '{}' ({} consecutive failures);"
                    + " reporting the last observed status {} instead", serviceName, failures, last, t);
            return last;
        }
    }

    @Override
    public Map<String, String> details() {
        Map<String, String> details;
        try {
            // Copy inside the boundary. Handing the delegate's own Map instance to the supervisor
            // hands it an object loaded by the isolated loader, and anything that keeps the
            // details — a status response, a log line held in a buffer — pins the loader.
            details = callWithContextLoader(loader, () -> {
                Map<String, String> reported = delegate.details();
                return reported == null
                        ? new LinkedHashMap<String, String>()
                        : new LinkedHashMap<>(reported);
            });
        } catch (Throwable t) {
            // Throwable for the same reason as status(): both are called after close().
            restoreInterruptIfInterrupted(t);
            LOG.error("Could not read the details of service '{}'", serviceName, t);
            details = new LinkedHashMap<>(Map.of("error", String.valueOf(t)));
        }
        return describeStatusStaleness(details);
    }

    /**
     * Adds the one thing {@link #status()} cannot say for itself: that the value it is reporting
     * is the last one observed rather than a fresh one.
     *
     * <p>Without this a service whose loader has gone reports {@code RUNNING} to JMX and to {@code
     * bin/cassandra-opensearch status} indefinitely, and nothing anywhere distinguishes it from a
     * service that is genuinely running. It goes in {@code details()} rather than into the status
     * itself precisely because nothing automated reads {@code details()} — the supervisor's health
     * check acts on {@code status()}, and a status that meant two things would make it act on the
     * wrong one.
     */
    private Map<String, String> describeStatusStaleness(Map<String, String> details) {
        int failures = consecutiveStatusFailures.get();
        if (failures > 0) {
            details.put("status_stale", "true");
            details.put("status_read_failures", String.valueOf(failures));
        }
        return details;
    }

    /**
     * Puts back an interrupt that was consumed by catching the {@link InterruptedException} that
     * cleared it. {@link #status()} and {@link #details()} may run on any thread, including one
     * the supervisor is in the middle of shutting down, and swallowing its interrupt leaves it
     * waiting for a stop signal that has already been sent.
     *
     * <p>The whole chain is walked, not just the top: an {@code InterruptedException} that reaches
     * here is usually already wrapped — Netty's {@code PlatformDependent.throwException} is inside
     * both real loaders, and both runtimes wrap what their executors throw — and looking only at
     * the outermost type left the flag clear in exactly the common case. Bounded and
     * cycle-checked, because the chain is assembled by code this project does not own.
     */
    private static void restoreInterruptIfInterrupted(Throwable t) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        int depth = 0;
        for (Throwable current = t;
             current != null && seen.add(current) && ++depth <= MAX_CAUSE_DEPTH;
             current = current.getCause()) {
            if (current instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                return;
            }
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
     * <p>This releases the loader's jar handles. It does not, in practice, make the loader
     * collectable: neither runtime's loader can be garbage collected after its service has
     * stopped, for reasons measured and recorded in {@code docs/KNOWN-GAPS.md} §4. Both halves
     * still have to happen — a service that is stopped but not closed holds its jars open, and
     * one that is closed but not stopped holds its threads and sockets.
     *
     * <p>Both failures are reported: if {@code stop()} fails and the loader then also fails to
     * close, the second is attached to the first as a suppressed exception rather than replacing
     * it.
     */
    @Override
    public void close() throws Exception {
        Throwable failure = null;
        try {
            // stop() unconditionally, relying on the idempotency the SPI requires. Skipping it
            // for terminal states looks like an optimisation but leaks: DECOMMISSIONED and
            // FAILED are both terminal, and both describe a service that still holds an open
            // node — a decommissioned OpenSearch node has left the cluster but its Node object,
            // threads and sockets are all still live, and a service that failed during start
            // may hold whatever it managed to allocate first.
            stop();
        } catch (Throwable t) {
            failure = t;
        }
        try {
            loader.close();
        } catch (Throwable t) {
            // Not `finally { loader.close(); }`: a throw from the finally block replaces the
            // failure from stop() and the reason the service would not shut down is lost —
            // which is the more interesting of the two, since a loader that will not close is
            // usually a consequence of it.
            if (failure == null) {
                failure = t;
            } else {
                failure.addSuppressed(t);
            }
        }
        if (failure != null) {
            throw rethrowable(serviceName, failure);
        }
    }

    /** Re-presents a caught throwable in a form {@code throws Exception} can carry. */
    private static Exception rethrowable(String serviceName, Throwable failure) throws Error {
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof Exception exception) {
            return exception;
        }
        return new ServiceException(serviceName, String.valueOf(failure.getMessage()), failure);
    }

    /** Exposed for tests that assert isolation properties. */
    public IsolatedClassLoader classLoader() {
        return loader;
    }

    // --- TCCL plumbing ---------------------------------------------------------------------

    private interface IsolatedAction {
        void run() throws Exception;
    }

    private static void runWithContextLoader(IsolatedClassLoader loader, IsolatedAction action) throws Exception {
        callWithContextLoader(loader, () -> {
            action.run();
            return null;
        });
    }

    /**
     * Runs {@code action} with the isolated loader installed as the context ClassLoader, and
     * translates whatever it throws into something the supervisor can hold — see the class
     * javadoc for why both halves are needed.
     */
    private static <T> T callWithContextLoader(IsolatedClassLoader loader, Callable<T> action) throws Exception {
        Thread current = Thread.currentThread();
        ClassLoader previous = current.getContextClassLoader();
        current.setContextClassLoader(loader);
        try {
            return action.call();
        } catch (Throwable t) {
            throw translate(loader.serviceName(), t);
        } finally {
            current.setContextClassLoader(previous);
        }
    }

    /**
     * @return the exception to throw at the supervisor, or throws it directly when it is an
     *         {@link Error} that may pass through. Never returns normally without a value.
     */
    private static Exception translate(String serviceName, Throwable thrown) throws Error {
        if (thrown instanceof ServiceException serviceException) {
            // Already the reportable form: the constructor flattened its cause chain, and
            // Throwable forbids overwriting a cause that was set there, so it cannot have
            // acquired an unloadable one since.
            return serviceException;
        }
        if (isReportableToSupervisor(thrown)) {
            if (thrown instanceof Error error) {
                // OutOfMemoryError, StackOverflowError and friends: JVM-level conditions that
                // the supervisor can already name. Wrapping one in a checked ServiceException
                // would only invite something to catch it.
                throw error;
            }
            if (thrown instanceof Exception exception) {
                return exception;
            }
        }
        return new ServiceException(
                serviceName,
                "Service '" + serviceName + "' failed with "
                        + thrown.getClass().getName() + ": " + thrown.getMessage(),
                thrown);
    }

    /**
     * @return true if {@code thrown} and every throwable in its cause chain resolve, by name, to
     *         the very classes the supervisor's own loader would resolve. Anything else must be
     *         flattened: the supervisor cannot print it without a {@link NoClassDefFoundError},
     *         and holding onto it holds the isolated loader alive.
     */
    private static boolean isReportableToSupervisor(Throwable thrown) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        int depth = 0;
        for (Throwable current = thrown; current != null; current = current.getCause()) {
            // The chain may be cyclic and may be arbitrarily long; see ServiceException#flatten.
            if (!seen.add(current) || ++depth > MAX_CAUSE_DEPTH) {
                return false;
            }
            if (!isLoadableBySupervisor(current.getClass())) {
                return false;
            }
        }
        return true;
    }

    private static boolean isLoadableBySupervisor(Class<?> type) {
        try {
            // Same name is not enough: two loaders can each hold a class of that name, and the
            // supervisor's copy is a different type from the one in hand.
            return Class.forName(type.getName(), false, IsolatedService.class.getClassLoader()) == type;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }
}
