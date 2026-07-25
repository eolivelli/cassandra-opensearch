/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.bootstrap;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A parent-last ClassLoader that gives one embedded server a private view of its own
 * dependencies.
 *
 * <p>This is the mechanism that lets Cassandra and OpenSearch coexist in one JVM despite
 * disagreeing on Netty, Guava, Jackson, Lucene and Log4j versions. It reproduces the technique
 * Cassandra's own {@code dtest} framework uses to run several nodes per JVM (see
 * {@code InstanceClassLoader} in the Cassandra tree); the dtest artifacts themselves are not
 * used, since they are test jars with shaded dependencies.
 *
 * <h2>Delegation</h2>
 *
 * The parent is the <b>platform</b> ClassLoader, not the application ClassLoader. That is the
 * whole trick: platform resolves {@code java.*}, {@code javax.*} and the rest of the JDK, but
 * knows nothing of the application classpath, so ordinary parent-first delegation already keeps
 * the supervisor's classes out. Only two categories reach outside:
 *
 * <ul>
 *   <li>JDK classes — via the platform parent, as usual.</li>
 *   <li>Classes matching {@link #sharedClassFilter} — delegated explicitly to the
 *       {@code sharedLoader}. In practice this is exactly {@code io.cassandraopensearch.spi.*},
 *       the bridge contract. It must stay this narrow: every shared package is a package the
 *       supervisor and both servers are forced to agree on.</li>
 * </ul>
 *
 * <p>Note that {@code org.slf4j} is deliberately <b>not</b> shared. Cassandra binds slf4j to
 * logback and OpenSearch binds it to log4j2; sharing the API would force one binding on both
 * and produce a single, confused log configuration. Each loader gets its own API and binding.
 *
 * <h2>Lifetime</h2>
 *
 * The loader must be {@link #close() closed} after its service has stopped, and nothing may
 * retain a reference to it or to any class it loaded, or the loader — and the entire server's
 * worth of classes and static state behind it — leaks.
 *
 * <p><b>Closing releases the jars; it does not make the loader collectable.</b> In practice
 * neither the Cassandra nor the OpenSearch loader can be garbage collected after its service has
 * stopped — measured, with the retaining roots named as far as they could be identified, in
 * {@code docs/KNOWN-GAPS.md} §4. That is harmless as shipped, because the process builds each
 * loader exactly once; it becomes a metaspace leak the moment anything rebuilds one, which is
 * why a restart must happen <i>inside</i> the existing loader.
 *
 * <p>This class is parallel-capable and safe for concurrent class loading.
 */
public final class IsolatedClassLoader extends URLClassLoader {

    static {
        ClassLoader.registerAsParallelCapable();
    }

    /**
     * The only package shared with the supervisor. Kept as a constant so tests and the assembly
     * can assert against the same value.
     */
    public static final String SPI_PACKAGE = "io.cassandraopensearch.spi.";

    private final String serviceName;
    private final ClassLoader sharedLoader;
    private final Predicate<String> sharedClassFilter;

    /**
     * @param serviceName       short name, used in {@link #toString()} and thread names
     * @param classpath         the jars this service may see; must be complete, since nothing
     *                          falls back to the application classpath
     * @param sharedLoader      the loader holding the SPI classes, normally the application loader
     * @param sharedClassFilter returns true for class names that must resolve to the
     *                          {@code sharedLoader}'s copy rather than a private one
     */
    public IsolatedClassLoader(
            String serviceName,
            List<URL> classpath,
            ClassLoader sharedLoader,
            Predicate<String> sharedClassFilter) {
        super(
                "isolated-" + Objects.requireNonNull(serviceName, "serviceName"),
                classpath.toArray(new URL[0]),
                ClassLoader.getPlatformClassLoader());
        this.serviceName = serviceName;
        this.sharedLoader = Objects.requireNonNull(sharedLoader, "sharedLoader");
        this.sharedClassFilter = Objects.requireNonNull(sharedClassFilter, "sharedClassFilter");
    }

    /**
     * Builds a loader with the default sharing policy: the SPI package and nothing else.
     */
    public static IsolatedClassLoader forService(String serviceName, List<URL> classpath) {
        return new IsolatedClassLoader(
                serviceName,
                classpath,
                IsolatedClassLoader.class.getClassLoader(),
                IsolatedClassLoader::isSpiClass);
    }

    /** @return true if {@code className} belongs to the shared bridge contract. */
    public static boolean isSpiClass(String className) {
        return className.startsWith(SPI_PACKAGE);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null) {
                loaded = sharedClassFilter.test(name)
                        // The bridge contract: both sides must see the identical Class object,
                        // otherwise every cross-boundary call fails with ClassCastException.
                        ? sharedLoader.loadClass(name)
                        // Everything else: platform parent for the JDK, then our own jars.
                        // Never the application classpath.
                        : super.loadClass(name, false);
            }
            if (resolve) {
                resolveClass(loaded);
            }
            return loaded;
        }
    }

    @Override
    public URL getResource(String name) {
        // Resources follow classes. The platform parent carries no application resources, so
        // the inherited lookup is already private to this loader; the SPI is class-only and
        // ships no resources, so there is nothing to delegate outward.
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        return super.getResources(name);
    }

    /** The service this loader belongs to, e.g. {@code "cassandra"}. */
    public String serviceName() {
        return serviceName;
    }

    /** Read-only view of the packages this loader shares with the supervisor. */
    public static Set<String> sharedPackages() {
        return Collections.singleton(SPI_PACKAGE);
    }

    @Override
    public String toString() {
        return "IsolatedClassLoader[" + serviceName + "]";
    }
}
