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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeConfigurationTest {

    @TempDir
    Path home;

    private NodeConfiguration load(String yaml) throws IOException {
        Path conf = Files.createDirectories(home.resolve("conf")).resolve("cassandra-opensearch.yaml");
        Files.writeString(conf, yaml);
        return NodeConfiguration.load(home);
    }

    private static final String MINIMAL = """
            services:
              cassandra: {}
              opensearch: {}
            """;

    @Test
    void appliesDefaultsForAMinimalFile() throws Exception {
        NodeConfiguration configuration = load(MINIMAL);

        assertThat(configuration.clusterName()).isEqualTo("cassandra-opensearch");
        assertThat(configuration.dataDirectory()).isEqualTo(home.resolve("data"));
        assertThat(configuration.logsDirectory()).isEqualTo(home.resolve("logs"));
        assertThat(configuration.healthCheckInterval()).isEqualTo(Duration.ofSeconds(10));

        ServiceConfiguration cassandra = configuration.service("cassandra");
        assertThat(cassandra.enabled()).isTrue();
        assertThat(cassandra.configFile()).isEqualTo(home.resolve("conf/cassandra.yaml"));
        assertThat(cassandra.libDirectory()).isEqualTo(home.resolve("lib/cassandra"));
        // Key names are the runtimes', not the YAML's; ServiceSettingsTest pins the full set.
        assertThat(cassandra.settings()).containsEntry("jmx.port", "7199");

        ServiceConfiguration opensearch = configuration.service("opensearch");
        assertThat(opensearch.configFile()).isEqualTo(home.resolve("conf/opensearch.yml"));
        assertThat(opensearch.libDirectory()).isEqualTo(home.resolve("lib/opensearch"));
        assertThat(opensearch.settings()).containsEntry("http_port", "9200");
    }

    @Test
    void servicesKeepStartupOrderWithCassandraFirst() throws Exception {
        // OpenSearch is listed first in the file; the supervisor must still start Cassandra
        // first, because OpenSearch's node identity derives from the Cassandra node.
        NodeConfiguration configuration = load("""
                services:
                  opensearch: {}
                  cassandra: {}
                """);

        assertThat(configuration.services().keySet()).containsExactly("cassandra", "opensearch");
    }

    @Test
    void readsExplicitPortsAndTimeouts() throws Exception {
        NodeConfiguration configuration = load("""
                cluster_name: prod-eu
                services:
                  cassandra:
                    jmx_port: 17199
                    native_transport_port: 19042
                    startup_timeout: 90s
                  opensearch:
                    http_port: 19200
                    startup_timeout: 3m
                decommission:
                  shard_relocation_timeout: 45m
                  ring_streaming_timeout: 2h
                """);

        assertThat(configuration.clusterName()).isEqualTo("prod-eu");
        assertThat(configuration.service("cassandra").settings()).containsEntry("jmx.port", "17199");
        assertThat(configuration.service("cassandra").startupTimeout()).isEqualTo(Duration.ofSeconds(90));
        assertThat(configuration.service("opensearch").startupTimeout()).isEqualTo(Duration.ofMinutes(3));
        assertThat(configuration.decommission().shardRelocationTimeout()).isEqualTo(Duration.ofMinutes(45));
        assertThat(configuration.decommission().ringStreamingTimeout()).isEqualTo(Duration.ofHours(2));
    }

    @Test
    void rejectsAnUnknownSettingRatherThanIgnoringIt() {
        // The failure mode this prevents: an operator sets a timeout, the key is misspelled, the
        // process starts happily, and the timeout they think they configured is not in force.
        assertThatThrownBy(() -> load("""
                services:
                  cassandra:
                    jmx_prot: 7199
                  opensearch: {}
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("jmx_prot")
                .hasMessageContaining("Known settings here");
    }

    /**
     * S5. {@code directories} was the one section that did not reject what it did not know.
     *
     * <p>The consequence is quiet and expensive: {@code dta:} is accepted, {@code data} keeps its
     * default, and the node's SSTables and commit log go under the installation directory — which
     * the next upgrade replaces. The README, the shipped configuration file and this class's own
     * javadoc all promise that cannot happen.
     */
    @Test
    void rejectsATypoInTheDirectoriesSection() {
        assertThatThrownBy(() -> load("""
                directories:
                  dta: /var/lib/cassandra-opensearch
                  logs: /var/log/cassandra-opensearch
                services:
                  cassandra: {}
                  opensearch: {}
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("dta")
                .hasMessageContaining("directories")
                .hasMessageContaining("Known settings here");
    }

    @Test
    void readsTheDirectoriesItDoesKnow() throws Exception {
        NodeConfiguration configuration = load("""
                directories:
                  data: var/data
                  logs: var/logs
                services:
                  cassandra: {}
                  opensearch: {}
                """);

        assertThat(configuration.dataDirectory()).isEqualTo(home.resolve("var/data"));
        assertThat(configuration.logsDirectory()).isEqualTo(home.resolve("var/logs"));
    }

    @Test
    void rejectsATopLevelTypo() {
        assertThatThrownBy(() -> load("""
                clustr_name: oops
                services:
                  cassandra: {}
                  opensearch: {}
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("clustr_name");
    }

    @Test
    void rejectsADurationWithoutAUnit() {
        // '30' could plausibly be seconds or milliseconds; guessing produces timeouts that are
        // wrong by three orders of magnitude, so it is refused outright.
        assertThatThrownBy(() -> load("""
                services:
                  cassandra:
                    startup_timeout: 30
                  opensearch: {}
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("must carry a unit");
    }

    @Test
    void rejectsAnOutOfRangePort() {
        assertThatThrownBy(() -> load("""
                services:
                  cassandra:
                    jmx_port: 99999
                  opensearch: {}
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("between 1 and 65535");
    }

    /**
     * A fractional port is a typo, and truncating it produces a node listening somewhere the
     * operator did not ask for. {@code Number.intValue()} used to do exactly that.
     */
    @Test
    void rejectsANumberThatIsNotWhole() {
        assertThatThrownBy(() -> load("""
                services:
                  cassandra: {}
                  opensearch:
                    http_port: 9200.7
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("http_port")
                .hasMessageContaining("whole number")
                .hasMessageContaining("9200.7");
    }

    /**
     * And a value too large for an {@code int} used to wrap round: {@code 4294967296} narrowed to
     * {@code 0}, which the port check then reported as "found 0" — naming a value nobody wrote and
     * sending the operator looking for a bug that is not there.
     */
    @Test
    void rejectsAnIntegerTooLargeToNarrowInsteadOfWrappingIt() {
        assertThatThrownBy(() -> load("""
                services:
                  cassandra: {}
                  opensearch:
                    http_port: 4294967296
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("http_port")
                .hasMessageContaining("4294967296");
    }

    /**
     * Zero reached {@code scheduleWithFixedDelay} and failed there with a bare
     * {@code IllegalArgumentException: null}, from a stack that names neither the file nor the key.
     */
    @Test
    void rejectsANonPositiveDuration() {
        assertThatThrownBy(() -> load("""
                services:
                  cassandra: {}
                  opensearch: {}
                supervisor:
                  health_check_interval: 0s
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("health_check_interval")
                .hasMessageContaining("positive duration");

        assertThatThrownBy(() -> load("""
                services:
                  cassandra:
                    startup_timeout: -5m
                  opensearch: {}
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("startup_timeout")
                .hasMessageContaining("positive duration");
    }

    /**
     * Duplicate keys and syntax errors arrive from snakeyaml as snakeyaml's own exceptions. Left
     * unwrapped they escape past every caller's {@code ConfigurationException} handler — including
     * {@code CassandraOpenSearchServer.run}, which then prints a parser stack trace instead of the
     * message. The position snakeyaml worked out is the useful half and has to survive.
     */
    @Test
    void rejectsDuplicateKeys() {
        assertThatThrownBy(() -> load("""
                cluster_name: first
                cluster_name: second
                services:
                  cassandra: {}
                  opensearch: {}
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("cassandra-opensearch.yaml")
                .hasMessageContaining("duplicate key")
                .hasMessageContaining("cluster_name")
                .hasMessageContaining("line 2");
    }

    @Test
    void reportsAYamlSyntaxErrorWithItsPosition() {
        assertThatThrownBy(() -> load("""
                services:
                  cassandra: {}
                   opensearch: {}
                """))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("cassandra-opensearch.yaml")
                .hasMessageContaining("not valid YAML")
                .hasMessageContaining("line 3");
    }

    @Test
    void reportsAMissingFileWithItsPath() {
        assertThatThrownBy(() -> NodeConfiguration.load(home))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("cassandra-opensearch.yaml");
    }

    @Test
    void requiresTheServicesSection() throws Exception {
        assertThatThrownBy(() -> load("cluster_name: lonely\n"))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("services");
    }

    @Test
    void allowsDisablingAServiceToIsolateAProblem() throws Exception {
        NodeConfiguration configuration = load("""
                services:
                  cassandra: {}
                  opensearch:
                    enabled: false
                """);

        assertThat(configuration.service("cassandra").enabled()).isTrue();
        assertThat(configuration.service("opensearch").enabled()).isFalse();
    }
}
