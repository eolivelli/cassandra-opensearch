/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.server.config.NodeConfiguration;
import io.cassandraopensearch.spi.EmbeddedService;
import io.cassandraopensearch.spi.ServiceStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The ordering of the coupled decommission.
 *
 * <p>This is the test that matters most in this module. Every individual call here already works
 * — both runtimes have their own tests — and what is left to get wrong is the sequence, which is
 * also the thing that silently loses data when it is wrong: an OpenSearch node that has not
 * finished relocating when Cassandra leaves the ring takes its primaries with it, and nothing
 * about the outcome looks like a failure.
 */
class DecommissionCoordinatorTest {

    @TempDir
    Path home;

    private final List<String> calls = new ArrayList<>();
    private final RecordingService cassandra = new RecordingService("cassandra", calls);
    private final RecordingService opensearch = new RecordingService("opensearch", calls);
    private final List<String> progress = new ArrayList<>();

    private static final String CONFIGURATION = """
            services:
              cassandra:
                shutdown_timeout: 5s
              opensearch:
                shutdown_timeout: 5s
            decommission:
              shard_relocation_timeout: 200ms
              ring_streaming_timeout: 400ms
            """;

    private DecommissionCoordinator coordinator() throws IOException {
        return coordinator(CONFIGURATION, Map.of("cassandra", cassandra, "opensearch", opensearch));
    }

    private DecommissionCoordinator coordinator(String yaml, Map<String, EmbeddedService> services)
            throws IOException {
        Files.createDirectories(home.resolve("conf"));
        Files.writeString(home.resolve("conf").resolve("cassandra-opensearch.yaml"), yaml);
        NodeConfiguration configuration = NodeConfiguration.load(home);
        Map<String, EmbeddedService> ordered = new LinkedHashMap<>();
        services.forEach(ordered::put);
        return new DecommissionCoordinator(configuration, ordered,
                (phase, percent, message) -> progress.add(phase + ": " + message));
    }

    @Test
    void runsTheFourPhasesInTheOnlyOrderThatDoesNotLoseData() throws Exception {
        String summary = coordinator().run(null, false);

        assertThat(calls).containsExactly(
                // 1. both services are told to stop taking on new ownership, so the two
                //    hand-offs overlap rather than running back to back
                "opensearch.prepareDecommission",
                "cassandra.prepareDecommission",
                // 2. OpenSearch relocates every shard off this node — the long wait
                "opensearch.awaitDecommissionReady",
                // 3. only now may the node leave, and OpenSearch leaves first: Cassandra's
                //    departure is the irreversible one
                "opensearch.decommission",
                "cassandra.decommission",
                // 4. stop, dependent service first
                "opensearch.stop",
                "cassandra.stop");

        assertThat(summary).contains("decommission complete");
        assertThat(cassandra.status()).isEqualTo(ServiceStatus.DECOMMISSIONED);
        assertThat(opensearch.status()).isEqualTo(ServiceStatus.DECOMMISSIONED);
    }

    @Test
    void reportsProgressSoTheCliCanRenderIt() throws Exception {
        coordinator().run(null, false);

        assertThat(progress).isNotEmpty();
        assertThat(progress).last().asString().contains("decommission complete");
    }

    @Test
    void refusesToLetCassandraLeaveWhileOpenSearchStillHoldsShards() throws Exception {
        opensearch.handsOffCleanly = false;

        assertThatThrownBy(() -> coordinator().run(null, false))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("did not finish relocating its shards")
                .hasMessageContaining("Cassandra has NOT left the ring")
                .hasMessageContaining("--force");

        // The point of the refusal: Cassandra never leaves, so nothing is lost and the operator
        // can retry once the cluster has caught up.
        assertThat(calls).containsExactly(
                "opensearch.prepareDecommission",
                "cassandra.prepareDecommission",
                "opensearch.awaitDecommissionReady");
    }

    @Test
    void forceProceedsWithShardsOutstanding() throws Exception {
        opensearch.handsOffCleanly = false;

        String summary = coordinator().run(null, true);

        assertThat(calls).containsExactly(
                "opensearch.prepareDecommission",
                "cassandra.prepareDecommission",
                "opensearch.awaitDecommissionReady",
                "opensearch.decommission",
                "cassandra.decommission",
                "opensearch.stop",
                "cassandra.stop");
        assertThat(summary).contains("forced out with shards outstanding");
    }

    @Test
    void honoursAndReportsTheShardRelocationTimeout() throws Exception {
        // The service burns its whole deadline and then gives up, which is what the OpenSearch
        // runtime does when shards will not move.
        opensearch.handsOffCleanly = false;
        opensearch.waitsForItsDeadline = true;

        long startedAt = System.nanoTime();
        assertThatThrownBy(() -> coordinator().run(null, false))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("within 200ms");

        assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                .isGreaterThanOrEqualTo(Duration.ofMillis(200));
        assertThat(opensearch.phaseTimeouts).containsEntry("awaitDecommissionReady", Duration.ofMillis(200));
        assertThat(cassandra.phaseTimeouts).containsEntry("prepareDecommission", Duration.ofMillis(400));
    }

    @Test
    void anExplicitTimeoutReplacesTheConfiguredPerPhaseLimits() throws Exception {
        // `--timeout 5s` means "give up after five seconds on each wait", not "share five
        // seconds between them".
        coordinator().run(Duration.ofSeconds(5), false);

        assertThat(opensearch.phaseTimeouts).containsEntry("awaitDecommissionReady", Duration.ofSeconds(5));
        assertThat(cassandra.phaseTimeouts).containsEntry("decommission", Duration.ofSeconds(5));
    }

    @Test
    void decommissionsANodeThatOnlyRunsCassandra() throws Exception {
        DecommissionCoordinator coordinator =
                coordinator(CONFIGURATION, Map.of("cassandra", cassandra));

        coordinator.run(null, false);

        assertThat(calls).containsExactly(
                "cassandra.prepareDecommission", "cassandra.decommission", "cassandra.stop");
    }

    @Test
    void refusesWhenNothingIsRunning() throws Exception {
        DecommissionCoordinator coordinator = coordinator(CONFIGURATION, Map.of());

        assertThatThrownBy(() -> coordinator.run(null, false))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("Nothing to decommission");
    }
}
