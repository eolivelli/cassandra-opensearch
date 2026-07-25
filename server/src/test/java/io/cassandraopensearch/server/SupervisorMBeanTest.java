/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.server.config.NodeConfiguration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * The control plane {@code bin/cassandra-opensearch status|decommission} talks to.
 *
 * <p>Exercised through the platform MBean server rather than by calling {@link SupervisorControl}
 * directly, because half of what can go wrong is registration: a getter the introspector does not
 * recognise as an attribute, or a return type a remote client could not deserialize, is invisible
 * to a direct call.
 */
class SupervisorMBeanTest {

    @TempDir
    Path home;

    private final List<String> calls = new ArrayList<>();
    private final RecordingService cassandra = new RecordingService("cassandra", calls);
    private final RecordingService opensearch = new RecordingService("opensearch", calls);

    private final MBeanServer platform = ManagementFactory.getPlatformMBeanServer();
    private ObjectName name;
    private Supervisor supervisor;

    @BeforeEach
    void startSupervisor() throws Exception {
        Files.createDirectories(home.resolve("conf"));
        Files.writeString(home.resolve("conf").resolve("cassandra-opensearch.yaml"), """
                cluster_name: prod-eu
                services:
                  cassandra:
                    shutdown_timeout: 5s
                  opensearch:
                    shutdown_timeout: 5s
                decommission:
                  shard_relocation_timeout: 200ms
                  ring_streaming_timeout: 200ms
                """);
        cassandra.hostId = "host-1";
        supervisor = new Supervisor(NodeConfiguration.load(home), service -> switch (service.name()) {
            case "cassandra" -> cassandra;
            default -> opensearch;
        });
        supervisor.start();
        name = new ObjectName(Supervisor.MBEAN_NAME);
    }

    @AfterEach
    void stopSupervisor() {
        supervisor.stop();
    }

    @Test
    void registersUnderTheProductDomainOnThePlatformServer() throws Exception {
        // The platform server is free: Cassandra keeps org.apache.cassandra:* in a private
        // MBeanServer of its own precisely so this namespace stays uncontested.
        assertThat(platform.isRegistered(name)).isTrue();
        assertThat(name.getDomain()).isEqualTo("io.cassandraopensearch");
    }

    @Test
    void exposesOverallStateAndPerServiceStatusAndDetails() throws Exception {
        assertThat(platform.getAttribute(name, "State")).isEqualTo("RUNNING");
        assertThat(platform.getAttribute(name, "ClusterName")).isEqualTo("prod-eu");
        assertThat((String[]) platform.getAttribute(name, "ServiceNames"))
                .containsExactly("cassandra", "opensearch");

        @SuppressWarnings("unchecked")
        Map<String, String> statuses =
                (Map<String, String>) platform.getAttribute(name, "ServiceStatuses");
        assertThat(statuses).containsEntry("cassandra", "RUNNING").containsEntry("opensearch", "RUNNING");

        @SuppressWarnings("unchecked")
        Map<String, String> details = (Map<String, String>) platform.invoke(
                name, "serviceDetails", new Object[]{"cassandra"}, new String[]{String.class.getName()});
        assertThat(details).containsEntry("hostId", "host-1");
    }

    @Test
    void unregistersOnShutdownSoARestartCanRegisterAgain() {
        supervisor.stop();

        assertThat(platform.isRegistered(name)).isFalse();
    }

    @Test
    void decommissionIsReachableAsAnOperationAndTakesTheProcessDown() throws Exception {
        Object summary = platform.invoke(name, "decommission",
                new Object[]{0L, false},
                new String[]{long.class.getName(), boolean.class.getName()});

        assertThat(summary).asString().contains("decommission complete");
        assertThat(platform.getAttribute(name, "DecommissionProgress")).asString()
                .contains("100%");
        assertThat(calls).filteredOn(call -> call.endsWith(".stop"))
                .containsExactly("opensearch.stop", "cassandra.stop");

        // The process exits 0 shortly afterwards; the delay is what lets this JMX reply reach a
        // remote CLI before the JVM goes.
        int exitCode = assertTimeoutPreemptively(Duration.ofSeconds(15), supervisor::awaitShutdown);
        assertThat(exitCode).isZero();
    }
}
