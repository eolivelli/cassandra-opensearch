/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import io.cassandraopensearch.spi.EmbeddedService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the service the way the product runs it — in a parent-last child ClassLoader, with the
 * SPI shared and the thread context ClassLoader pinned — and checks the REST API in that shape.
 *
 * <p>This is the test that would have caught the spike's most expensive finding. On a flat
 * classpath every endpoint works, so {@link OpenSearchRestApiTest} cannot fail for the reason
 * this one exists: only under isolation does {@code XContentBuilder}'s
 * {@code ServiceLoader.load(Class)} lookup come up empty and poison its {@code static final}
 * writer table, after which {@code _cluster/health}, {@code _nodes/stats} and
 * {@code _cluster/stats} answer 400 while indexing and search stay perfectly healthy.
 *
 * <p>The loader below deliberately mirrors {@code IsolatedClassLoader} in the bootstrap module
 * rather than using it: this jar ships in {@code lib/opensearch/} and must not depend on
 * anything the supervisor owns, not even in tests.
 *
 * <p>Removing the pin in {@link #pinned} does make this test fail, though not with the product's
 * exact symptom: here the surrounding classpath also carries OpenSearch, so the misdirected
 * {@code ServiceLoader} finds the <i>other</i> loader's provider and dies loudly with
 * {@code ServiceConfigurationError: ... not a subtype}. In the product the supervisor's
 * classpath has no OpenSearch at all, the lookup simply returns nothing, and the damage is
 * silent.
 */
class IsolatedClassLoaderRestApiTest {

    private static final int HTTP_PORT = 19230;
    private static final int TRANSPORT_PORT = 19330;

    @Test
    void theRestApiIsIntactInsideAChildClassLoader(@TempDir Path home) throws Exception {
        List<URL> classpath = classpathUrls();
        assertThat(classpath)
                .as("surefire must expose the real classpath; see useManifestOnlyJar in the POM")
                .anyMatch(url -> url.getPath().contains("/opensearch/3."));

        try (URLClassLoader loader = new SpiSharingClassLoader(classpath)) {
            Class<?> isolatedType = Class.forName(OpenSearchService.class.getName(), true, loader);
            assertThat(isolatedType.getClassLoader()).isSameAs(loader);
            assertThat(isolatedType).isNotSameAs(OpenSearchService.class);

            // The cast succeeds because the SPI - and only the SPI - is shared with this loader.
            EmbeddedService service = (EmbeddedService) isolatedType.getDeclaredConstructor().newInstance();
            TestServiceContext context = new TestServiceContext(home, "isolated-node", HTTP_PORT, TRANSPORT_PORT);
            Rest rest = new Rest(HTTP_PORT);
            try {
                pinned(loader, () -> {
                    service.start(context);
                    return null;
                });

                assertThat(rest.put("/isolated/_doc/1?refresh=true", """
                        {"title":"a document written through a child loader"}""").status())
                        .isEqualTo(201);
                assertThat(rest.post("/isolated/_search", """
                        {"query":{"match":{"title":"child"}}}""").body())
                        .contains("a document written through a child loader");

                for (String path : new String[] {
                        "/_cluster/health?timeout=30s",
                        "/_nodes/stats",
                        "/_cluster/stats" }) {
                    Rest.Response response = rest.get(path);
                    assertThat(response.status()).as("GET %s -> %s", path, response.body()).isEqualTo(200);
                    assertThat(response.body()).doesNotContain("no raw transformer found");
                }

                assertThat(pinned(loader, service::details)).containsEntry("node.name", "isolated-node");
            } finally {
                pinned(loader, () -> {
                    service.stop();
                    return null;
                });
            }
        }
    }

    /**
     * What {@code IsolatedService} does around every SPI call. The pin has to cover
     * construction as well as use: OpenSearch builds its thread pools in {@code Node.<init>}
     * and worker threads inherit the creating thread's context ClassLoader.
     */
    private static <T> T pinned(ClassLoader loader, Callable<T> action) throws Exception {
        Thread current = Thread.currentThread();
        ClassLoader previous = current.getContextClassLoader();
        current.setContextClassLoader(loader);
        try {
            return action.call();
        } finally {
            current.setContextClassLoader(previous);
        }
    }

    private static List<URL> classpathUrls() throws Exception {
        List<URL> urls = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            urls.add(Paths.get(entry).toUri().toURL());
        }
        return urls;
    }

    /** Parent-last except for the JDK (via the platform parent) and the SPI bridge contract. */
    private static final class SpiSharingClassLoader extends URLClassLoader {

        private final ClassLoader spiLoader = EmbeddedService.class.getClassLoader();

        SpiSharingClassLoader(List<URL> classpath) {
            super("isolated-opensearch-test", classpath.toArray(new URL[0]), ClassLoader.getPlatformClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    loaded = name.startsWith("io.cassandraopensearch.spi.")
                            ? spiLoader.loadClass(name)
                            : super.loadClass(name, false);
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }
    }
}
