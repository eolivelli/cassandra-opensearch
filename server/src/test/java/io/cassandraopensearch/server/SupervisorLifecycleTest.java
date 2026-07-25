/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.server.config.NodeConfiguration;
import io.cassandraopensearch.spi.ServiceException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/** Startup order, shutdown order, and what happens when either goes wrong. */
class SupervisorLifecycleTest {

    @TempDir
    Path home;

    private final List<String> calls = new ArrayList<>();
    private final RecordingService cassandra = new RecordingService("cassandra", calls);
    private final RecordingService opensearch = new RecordingService("opensearch", calls);

    private Supervisor supervisor;

    @AfterEach
    void stopSupervisor() {
        if (supervisor != null) {
            supervisor.stop();
        }
    }

    private Supervisor supervisor(String yaml) throws IOException {
        Files.createDirectories(home.resolve("conf"));
        Files.writeString(home.resolve("conf").resolve("cassandra-opensearch.yaml"), yaml);
        NodeConfiguration configuration = NodeConfiguration.load(home);
        supervisor = new Supervisor(configuration, service -> switch (service.name()) {
            case "cassandra" -> cassandra;
            case "opensearch" -> opensearch;
            default -> throw new IllegalArgumentException(service.name());
        });
        return supervisor;
    }

    private static final String BOTH = """
            services:
              cassandra:
                startup_timeout: 5s
                shutdown_timeout: 5s
              opensearch:
                startup_timeout: 5s
                shutdown_timeout: 5s
            supervisor:
              health_check_interval: 50ms
            """;

    @Test
    void startsCassandraFirstAndStopsOpenSearchFirst() throws Exception {
        Supervisor supervisor = supervisor(BOTH);

        supervisor.start();

        assertThat(calls).containsExactly("cassandra.start", "opensearch.start");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.RUNNING);

        supervisor.stop();

        // OpenSearch is the dependent service, so it goes down first; each service's loader is
        // released immediately after it stops.
        assertThat(calls).containsExactly(
                "cassandra.start", "opensearch.start",
                "opensearch.stop", "opensearch.close",
                "cassandra.stop", "cassandra.close");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.STOPPED);
        assertThat(supervisor.awaitShutdown()).isZero();
    }

    @Test
    void stopsWhatAlreadyStartedWhenAServiceFailsToStart() throws Exception {
        opensearch.startFailure = new ServiceException("opensearch", "no route to the cluster manager");
        Supervisor supervisor = supervisor(BOTH);

        assertThatThrownBy(supervisor::start)
                .isInstanceOf(SupervisorException.class)
                .hasMessageContaining("opensearch")
                .hasMessageContaining("failed to start")
                .hasMessageContaining("no route to the cluster manager");

        // The failed service is stopped too: it is recorded before start() is called precisely
        // because a service that dies halfway through startup still holds what it allocated.
        assertThat(calls).containsExactly(
                "cassandra.start", "opensearch.start",
                "opensearch.stop", "opensearch.close",
                "cassandra.stop", "cassandra.close");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.FAILED);
        assertThat(supervisor.awaitShutdown()).isEqualTo(1);
    }

    @Test
    void skipsADisabledServiceEntirely() throws Exception {
        Supervisor supervisor = supervisor("""
                services:
                  cassandra: {}
                  opensearch:
                    enabled: false
                """);

        supervisor.start();
        supervisor.stop();

        assertThat(calls).containsExactly("cassandra.start", "cassandra.stop", "cassandra.close");
    }

    @Test
    void namesTheOpenSearchNodeAfterTheCassandraHostId() throws Exception {
        // The decommission shard exclusion keys on the OpenSearch node name, so it has to be an
        // identity that is stable across restarts and unique per node — Cassandra's host id,
        // not this machine's hostname.
        cassandra.hostId = "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9";
        Supervisor supervisor = supervisor(BOTH);

        supervisor.start();

        assertThat(opensearch.context().settings())
                .containsEntry("node_name", "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9");
        assertThat(cassandra.context().settings()).doesNotContainKey("node_name");
    }

    @Test
    void theShutdownHookIsIdempotentWithAnExplicitStop() throws Exception {
        Supervisor supervisor = supervisor(BOTH);
        supervisor.start();
        Thread hook = supervisor.shutdownHook();
        assertThat(hook).isNotNull();

        // Whichever of the two arrives first does the work; the other must be a no-op, or the
        // services get a second stop() after their loaders have already been discarded.
        hook.run();
        supervisor.stop();
        hook.run();

        assertThat(calls).containsExactly(
                "cassandra.start", "opensearch.start",
                "opensearch.stop", "opensearch.close",
                "cassandra.stop", "cassandra.close");
    }

    @Test
    void anExplicitStopBeforeTheHookLeavesTheHookWithNothingToDo() throws Exception {
        Supervisor supervisor = supervisor(BOTH);
        supervisor.start();
        Thread hook = supervisor.shutdownHook();

        supervisor.stop();
        hook.run();

        assertThat(calls).filteredOn(call -> call.endsWith(".stop"))
                .containsExactly("opensearch.stop", "cassandra.stop");
    }

    @Test
    void aFatalErrorFromAServiceShutsTheProcessDownInOrder() throws Exception {
        Supervisor supervisor = supervisor(BOTH);
        supervisor.start();

        // What a runtime calls when a background thread dies: "this service wants the process
        // dead". The supervisor must still take both services down in order, and report non-zero.
        cassandra.reportFatalError("commit log directory is unwritable");

        int exitCode = assertTimeoutPreemptively(Duration.ofSeconds(10), supervisor::awaitShutdown);

        assertThat(exitCode).isEqualTo(1);
        assertThat(supervisor.state()).isEqualTo(SupervisorState.FAILED);
        assertThat(supervisor.failureMessage()).contains("commit log directory is unwritable");
        assertThat(calls).filteredOn(call -> call.endsWith(".stop"))
                .containsExactly("opensearch.stop", "cassandra.stop");
    }

    @Test
    void healthSupervisionTurnsAFailedServiceIntoAnOrderedShutdown() throws Exception {
        Supervisor supervisor = supervisor(BOTH);
        supervisor.start();

        // No callback this time: the service just goes FAILED, and the health poll has to notice.
        opensearch.fail();

        int exitCode = assertTimeoutPreemptively(Duration.ofSeconds(10), supervisor::awaitShutdown);

        assertThat(exitCode).isEqualTo(1);
        assertThat(supervisor.failureMessage()).contains("opensearch").contains("FAILED");
        assertThat(calls).filteredOn(call -> call.endsWith(".stop"))
                .containsExactly("opensearch.stop", "cassandra.stop");
    }

    @Test
    void startMayOnlyBeCalledOnce() throws Exception {
        Supervisor supervisor = supervisor(BOTH);
        supervisor.start();

        assertThatThrownBy(supervisor::start)
                .isInstanceOf(SupervisorException.class)
                .hasMessageContaining("only be called once");
    }
}
