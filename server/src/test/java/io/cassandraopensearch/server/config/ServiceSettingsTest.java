/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the exact set of keys {@link ServiceConfiguration#settings()} hands to each runtime.
 *
 * <p>This is the one place in the product where two modules agree on a contract that the
 * compiler cannot check. The runtimes live in isolated ClassLoaders and are named by string, so
 * they cannot share a constant with the supervisor; they look their settings up out of a {@code
 * Map<String, String>} and fall back to a default when a key is absent. A rename on either side
 * therefore produces no error at all — just a supervisor setting that quietly stops having any
 * effect, and a node that comes up on a port nobody asked for.
 *
 * <p>The expected names below are copied from the runtimes' own constants:
 * {@code CassandraService.SETTING_JMX_ADDRESS}, {@code SETTING_JMX_PORT},
 * {@code SETTING_STARTUP_TIMEOUT_SECONDS}, and {@code OpenSearchService.SETTING_CLUSTER_NAME},
 * {@code SETTING_NODE_NAME}, {@code SETTING_HTTP_PORT}, {@code SETTING_TRANSPORT_PORT},
 * {@code SETTING_NETWORK_HOST}, {@code SETTING_START_TIMEOUT}. They are repeated as literals
 * because importing them would put a runtime class on the supervisor's classpath.
 */
class ServiceSettingsTest {

    @TempDir
    Path home;

    private NodeConfiguration load(String yaml) throws IOException {
        Files.createDirectories(home.resolve("conf"));
        Files.writeString(home.resolve("conf").resolve("cassandra-opensearch.yaml"), yaml);
        return NodeConfiguration.load(home);
    }

    private static final String MINIMAL = """
            services:
              cassandra: {}
              opensearch: {}
            """;

    @Test
    void cassandraGetsExactlyTheThreeKeysItsRuntimeReads() throws Exception {
        assertThat(load(MINIMAL).service("cassandra").settings())
                .containsOnlyKeys("jmx.address", "jmx.port", "startup.timeout.seconds")
                .containsEntry("jmx.address", "127.0.0.1")
                .containsEntry("jmx.port", "7199")
                .containsEntry("startup.timeout.seconds", "300");
    }

    @Test
    void opensearchGetsExactlyTheSixKeysItsRuntimeReads() throws Exception {
        assertThat(load(MINIMAL).service("opensearch").settings())
                .containsOnlyKeys("cluster_name", "node_name", "http_port", "transport_port",
                        "network_host", "start_timeout_seconds")
                .containsEntry("cluster_name", "cassandra-opensearch")
                .containsEntry("http_port", "9200")
                .containsEntry("transport_port", "9300")
                .containsEntry("network_host", "127.0.0.1")
                .containsEntry("start_timeout_seconds", "300");
    }

    @Test
    void theStartupTimeoutReachesBothRuntimesInTheUnitsTheyParse() throws Exception {
        // Both read seconds out of a plain string; the YAML is written with a unit, which is the
        // conversion this test exists to keep honest.
        NodeConfiguration configuration = load("""
                services:
                  cassandra:
                    startup_timeout: 90s
                  opensearch:
                    startup_timeout: 3m
                """);

        assertThat(configuration.service("cassandra").settings())
                .containsEntry("startup.timeout.seconds", "90");
        assertThat(configuration.service("opensearch").settings())
                .containsEntry("start_timeout_seconds", "180");
    }

    @Test
    void theClusterNameReachesOpenSearchFromTheTopLevelSetting() throws Exception {
        // Cassandra takes its cluster name from conf/cassandra.yaml, which it parses itself;
        // OpenSearch has no equivalent authority, so the supervisor supplies it.
        NodeConfiguration configuration = load("""
                cluster_name: prod-eu
                services:
                  cassandra: {}
                  opensearch: {}
                """);

        assertThat(configuration.service("opensearch").settings())
                .containsEntry("cluster_name", "prod-eu");
    }

    @Test
    void theNodeNameFallsBackToTheHostnameUntilCassandraSuppliesOne() throws Exception {
        // The supervisor overrides this with the Cassandra host id once Cassandra is up — see
        // SupervisorLifecycleTest. This value is what a node with Cassandra disabled gets.
        assertThat(load(MINIMAL).service("opensearch").settings())
                .containsEntry("node_name", InetAddress.getLocalHost().getHostName());
    }

    @Test
    void anExplicitNodeNameWins() throws Exception {
        NodeConfiguration configuration = load("""
                services:
                  cassandra: {}
                  opensearch:
                    node_name: search-07
                """);

        assertThat(configuration.service("opensearch").settings())
                .containsEntry("node_name", "search-07");
    }

    @Test
    void theJmxAddressIsSeparateFromTheListenAddress() throws Exception {
        // Deliberately not derived from listen_address: moving Cassandra's storage listener onto
        // a routable interface must not silently move its JMX port there too.
        NodeConfiguration configuration = load("""
                services:
                  cassandra:
                    listen_address: 10.0.0.7
                    jmx_address: 10.0.0.7
                    jmx_port: 17199
                  opensearch: {}
                """);

        assertThat(configuration.service("cassandra").settings())
                .containsEntry("jmx.address", "10.0.0.7")
                .containsEntry("jmx.port", "17199");
    }
}
