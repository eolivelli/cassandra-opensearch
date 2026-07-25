/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.cassandraopensearch.bootstrap.IsolatedService;

/**
 * A Cassandra node running the way the supervisor will run it: the real
 * {@link io.cassandraopensearch.bootstrap.IsolatedClassLoader}, the real {@link IsolatedService}
 * proxy, and the real {@link CassandraService} inside it. Nothing in the test JVM's application
 * ClassLoader can see a Cassandra class, which is what makes the CQL and JMX assertions meaningful.
 */
final class TestNode implements AutoCloseable {

    static final String ADDRESS = "127.0.0.1";

    /**
     * Named, not {@code CassandraService.class.getName()}: a class literal would load the service
     * in the application loader, where Cassandra is deliberately absent. The supervisor names it
     * the same way, from configuration.
     */
    private static final String IMPLEMENTATION_CLASS =
            "io.cassandraopensearch.runtime.cassandra.CassandraService";

    /**
     * Each node in this JVM gets its own port block. Ports are cheap and reuse after a stop is the
     * kind of thing that fails once in fifty runs on a loaded machine.
     */
    private static final AtomicInteger NODES_STARTED = new AtomicInteger();

    private final IsolatedService service;
    private final RecordingServiceContext context;
    private final Path home;
    private final int storagePort;
    private final int nativePort;
    private final int jmxPort;

    private TestNode(IsolatedService service, RecordingServiceContext context, Path home,
                     int storagePort, int nativePort, int jmxPort) {
        this.service = service;
        this.context = context;
        this.home = home;
        this.storagePort = storagePort;
        this.nativePort = nativePort;
        this.jmxPort = jmxPort;
    }

    /** Creates the node and its isolated loader, but does not start it. */
    static TestNode create(Path home) throws Exception {
        return create(home, ADDRESS);
    }

    /**
     * @param jmxAddress what to put in {@code cassandra.jmx.address}. Only the JMX connector reads
     *                   it; {@code cassandra.yaml} still binds {@link #ADDRESS}, so a value that
     *                   does not resolve fails {@link NodeJmxServer} and nothing else — which is
     *                   the point of being able to set it.
     */
    static TestNode create(Path home, String jmxAddress) throws Exception {
        int offset = NODES_STARTED.getAndIncrement() * 10;
        int storagePort = intProperty("test.cassandra.storagePort", 17000) + offset;
        int nativePort = intProperty("test.cassandra.nativePort", 19042) + offset;
        int jmxPort = intProperty("test.cassandra.jmxPort", 17199) + offset;

        RecordingServiceContext context = new RecordingServiceContext(home, Map.of(
                CassandraService.NAME + '.' + CassandraService.SETTING_JMX_ADDRESS, jmxAddress,
                CassandraService.NAME + '.' + CassandraService.SETTING_JMX_PORT, String.valueOf(jmxPort),
                CassandraService.NAME + '.' + CassandraService.SETTING_STARTUP_TIMEOUT_SECONDS, "180"));

        Files.createDirectories(context.configDirectory());
        Files.createDirectories(context.dataDirectory());
        Files.createDirectories(context.logsDirectory());
        writeConfiguration(context.configDirectory(), storagePort, nativePort);

        // Speed up the parts of startup that exist for real clusters. All cassandra.-namespaced,
        // all JVM-global, exactly as the production launcher would set them.
        System.setProperty("cassandra.ring_delay_ms", "5000");
        System.setProperty("cassandra.skip_wait_for_gossip_to_settle", "0");
        System.setProperty("cassandra.superuser_setup_delay_ms", "1000");

        IsolatedService service = IsolatedService.create(
                CassandraService.NAME,
                IMPLEMENTATION_CLASS,
                isolatedClasspath(context.configDirectory()));
        return new TestNode(service, context, home, storagePort, nativePort, jmxPort);
    }

    static TestNode started(Path home) throws Exception {
        TestNode node = create(home);
        try {
            node.service().start(node.context());
        } catch (Exception e) {
            node.close();
            throw e;
        }
        return node;
    }

    IsolatedService service() {
        return service;
    }

    RecordingServiceContext context() {
        return context;
    }

    int nativePort() {
        return nativePort;
    }

    int jmxPort() {
        return jmxPort;
    }

    String jmxServiceUrl() {
        return "service:jmx:rmi://" + ADDRESS + ':' + jmxPort + "/jndi/rmi://" + ADDRESS + ':' + jmxPort + "/jmxrmi";
    }

    Path logFile() {
        return home.resolve("logs").resolve("system.log");
    }

    ClassLoader isolatedLoader() {
        return service.classLoader();
    }

    @Override
    public void close() throws Exception {
        service.close();
    }

    // --- fixture plumbing ------------------------------------------------------------------

    /**
     * The classpath the distribution's {@code lib/cassandra/} directory will hold: this module's
     * classes, the whole Cassandra dependency tree, and {@code conf/} — the last of these because
     * that is where logback finds its configuration, and pointing at it with the JVM-global
     * {@code logback.configurationFile} property would reach OpenSearch's logging too.
     */
    private static List<URL> isolatedClasspath(Path configDirectory) throws Exception {
        List<URL> classpath = new ArrayList<>();
        classpath.add(moduleClassesDirectory().toUri().toURL());
        classpath.add(configDirectory.toUri().toURL());

        Path classpathFile = Path.of(System.getProperty("cassandra.classpath.file",
                "target/cassandra-classpath.txt"));
        for (String entry : Files.readString(classpathFile).trim().split(File.pathSeparator)) {
            if (!entry.isBlank()) {
                classpath.add(Path.of(entry).toUri().toURL());
            }
        }
        return classpath;
    }

    private static Path moduleClassesDirectory() throws Exception {
        Path testClasses = Path.of(
                TestNode.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        return testClasses.resolveSibling("classes");
    }

    private static void writeConfiguration(Path configDirectory, int storagePort, int nativePort)
            throws IOException {
        String yaml = resource("cassandra.yaml.template")
                .replace("@CLUSTER_NAME@", "cassandra-opensearch-test")
                .replace("@ADDRESS@", ADDRESS)
                .replace("@STORAGE_PORT@", String.valueOf(storagePort))
                .replace("@SSL_STORAGE_PORT@", String.valueOf(storagePort + 1))
                .replace("@NATIVE_PORT@", String.valueOf(nativePort));
        Files.writeString(configDirectory.resolve("cassandra.yaml"), yaml);
        Files.writeString(configDirectory.resolve("logback.xml"), resource("node-logback.xml"));
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = TestNode.class.getClassLoader().getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException("Missing test resource " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static int intProperty(String key, int defaultValue) {
        return Integer.parseInt(System.getProperty(key, String.valueOf(defaultValue)));
    }

    /** Convenience for assertions that read several details at once. */
    Map<String, String> details() {
        return new LinkedHashMap<>(service.details());
    }
}
