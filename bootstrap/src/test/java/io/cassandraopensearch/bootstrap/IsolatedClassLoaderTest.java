/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.bootstrap;

import io.cassandraopensearch.spi.EmbeddedService;
import io.cassandraopensearch.spi.ServiceStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the isolation properties the whole design rests on, using a service implementation
 * compiled at test time into a jar that is deliberately <b>not</b> on the test classpath.
 *
 * <p>Testing this with a class the test JVM can already see would prove nothing: every
 * assertion would pass against a supervisor-loaded copy while the isolation quietly did
 * nothing.
 */
class IsolatedClassLoaderTest {

    private static final String IMPL = "probe.ProbeService";

    private static Path jar;

    @BeforeAll
    static void buildProbeJar(@TempDir Path tempDir) throws Exception {
        jar = ProbeServiceCompiler.compileToJar(tempDir);
    }

    private static List<URL> classpath() throws Exception {
        return List.of(jar.toUri().toURL());
    }

    @Test
    void loadsTheImplementationInTheIsolatedLoaderNotTheApplicationLoader() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            Class<?> loaded = Class.forName(IMPL, false, service.classLoader());
            assertThat(loaded.getClassLoader())
                    .as("implementation must belong to the isolated loader")
                    .isSameAs(service.classLoader());
        }
    }

    @Test
    void applicationLoaderCannotSeeTheIsolatedImplementation() {
        assertThatThrownBy(() -> Class.forName(IMPL, false, getClass().getClassLoader()))
                .as("if the supervisor can load it, the jar leaked onto lib/boot")
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void spiTypesResolveToTheSameClassObjectOnBothSides() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            Class<?> insideView = Class.forName(EmbeddedService.class.getName(), false, service.classLoader());

            // This is the single most important property here. If the isolated loader resolved
            // its own copy of the SPI, every cross-boundary call would fail with
            // ClassCastException, and the supervisor could not hold the service at all.
            assertThat(insideView).isSameAs(EmbeddedService.class);
        }
    }

    @Test
    void jdkTypesStillResolveThroughThePlatformLoader() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            assertThat(Class.forName("java.lang.String", false, service.classLoader()))
                    .isSameAs(String.class);
            assertThat(Class.forName("java.sql.Date", false, service.classLoader()))
                    .isSameAs(java.sql.Date.class);
        }
    }

    @Test
    void twoServicesGetIndependentCopiesOfTheSameClass() throws Exception {
        try (IsolatedService first = IsolatedService.create("probe-a", IMPL, classpath());
             IsolatedService second = IsolatedService.create("probe-b", IMPL, classpath())) {

            Class<?> a = Class.forName(IMPL, false, first.classLoader());
            Class<?> b = Class.forName(IMPL, false, second.classLoader());

            // The point of the exercise: same name, same jar, two unrelated Class objects with
            // two unrelated sets of statics. This is what lets Cassandra and OpenSearch each
            // believe they own the JVM.
            assertThat(a).isNotSameAs(b);
            assertThat(a.getName()).isEqualTo(b.getName());
        }
    }

    @Test
    void lifecycleCallsCrossTheBoundaryAndSeeTheIsolatedContextClassLoader() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            assertThat(service.status()).isEqualTo(ServiceStatus.NEW);

            service.start(new RecordingServiceContext());
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);

            // The probe records the TCCL it observed inside start(). Cassandra and OpenSearch
            // both reach for it during startup, so getting this wrong breaks them in ways that
            // are painful to diagnose.
            assertThat(service.details())
                    .containsEntry("tccl", "isolated-probe")
                    .containsEntry("name", "probe");

            service.stop();
            assertThat(service.status()).isEqualTo(ServiceStatus.STOPPED);
        }
    }

    @Test
    void exceptionsFromInsideArriveAsLoadableTypes() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            context.settings().put("probe.failOnStart", "true");

            // The probe throws an exception class that exists only inside its own jar. The
            // supervisor must still get something it can load, print and log.
            assertThatThrownBy(() -> service.start(context))
                    .isInstanceOf(io.cassandraopensearch.spi.ServiceException.class)
                    .hasMessageContaining("probe was asked to fail");
        }
    }

    @Test
    void closingReleasesTheLoader() throws Exception {
        IsolatedService service = IsolatedService.create("probe", IMPL, classpath());
        service.start(new RecordingServiceContext());
        service.close();

        // probe.LazyProbe exists in the jar but was never loaded, so this reaches the jar file
        // rather than the loaded-class cache. A closed URLClassLoader has released its jar
        // handles and can no longer satisfy it.
        assertThatThrownBy(() -> Class.forName("probe.LazyProbe", false, service.classLoader()))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void applicationClasspathIsInvisibleToTheIsolatedLoader() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            // ClasspathResolver sits on the application classpath and is not in the probe jar.
            // Because the isolated loader's parent is the *platform* loader, the application
            // classpath is simply not reachable — this is what stops Cassandra's Guava from
            // being satisfied by the supervisor's, or vice versa.
            assertThatThrownBy(() ->
                    Class.forName(ClasspathResolver.class.getName(), false, service.classLoader()))
                    .isInstanceOf(ClassNotFoundException.class);

            // ...while the same name resolves perfectly well from the supervisor's side.
            assertThat(Class.forName(ClasspathResolver.class.getName(), false, getClass().getClassLoader()))
                    .isSameAs(ClasspathResolver.class);
        }
    }
}
