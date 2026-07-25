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
import io.cassandraopensearch.spi.ServiceException;
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
        // can retry once the cluster has caught up — with the node put back as it was, which is
        // what the two aborts are.
        assertThat(calls).containsExactly(
                "opensearch.prepareDecommission",
                "cassandra.prepareDecommission",
                "opensearch.awaitDecommissionReady",
                "cassandra.abortDecommission",
                "opensearch.abortDecommission");
        assertThat(opensearch.status()).isEqualTo(ServiceStatus.RUNNING);
        assertThat(cassandra.status()).isEqualTo(ServiceStatus.RUNNING);
    }

    /**
     * The defect this compensation exists for, in the shape it actually occurs.
     *
     * <p>Cassandra is the service that refuses a single-node decommission, and it is prepared
     * <i>second</i> — by then OpenSearch has already excluded this node from shard allocation
     * cluster-wide. Left in place that exclusion outlives the refusal, and on a single-node
     * cluster every index created afterwards stays UNASSIGNED forever: one mistyped command
     * produces what looks like an unrecoverable cluster. So the refusal must undo it, and it
     * must undo it in reverse order — the service prepared last is put back first.
     */
    @Test
    void aRefusedPreparationPutsEveryPreparedServiceBack() throws Exception {
        cassandra.prepareFailure = new ServiceException("cassandra",
                "this node is the only member of the ring");

        assertThatThrownBy(() -> coordinator().run(null, false))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("prepare-cassandra")
                .hasMessageContaining("only member of the ring");

        assertThat(calls).containsExactly(
                "opensearch.prepareDecommission",
                "cassandra.prepareDecommission",
                // The service whose own preparation threw is compensated too: it may have
                // applied half of what it does before failing.
                "cassandra.abortDecommission",
                "opensearch.abortDecommission");
        assertThat(opensearch.status()).isEqualTo(ServiceStatus.RUNNING);
    }

    /**
     * A cancelled decommission is exactly when the preparations most need undoing.
     *
     * <p>Cancelled <i>inside the shard-relocation wait</i>, which is where a cancellation really
     * arrives: it is the phase that takes minutes, so it is the phase a SIGTERM lands in. The
     * service notices through {@code isCancelled()} and reports that it gave up; the coordinator
     * has to recognise that as a cancellation rather than blaming the cluster for a slow
     * hand-off, and has to put both services back.
     */
    @Test
    void aCancelledDecommissionIsAlsoBackedOut() throws Exception {
        DecommissionCoordinator coordinator = coordinator();
        opensearch.onAwaitDecommissionReady = coordinator::cancel;

        assertThatThrownBy(() -> coordinator.run(null, false))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("cancelled")
                .hasMessageContaining("NOT left either cluster");

        assertThat(calls).containsExactly(
                "opensearch.prepareDecommission",
                "cassandra.prepareDecommission",
                "opensearch.awaitDecommissionReady",
                "cassandra.abortDecommission",
                "opensearch.abortDecommission");
        assertThat(opensearch.cancelledDuringAbort)
                .as("the compensating phase must not report itself cancelled, or a service could"
                        + " stop half way through putting the node back")
                .isFalse();
    }

    /**
     * The defect: {@code cancel()} used to set a flag that {@code run()} never read. Only
     * {@code awaitDecommissionReady} polls it, and {@code prepareDecommission} and
     * {@code decommission} correctly do not — so a run cancelled anywhere other than inside that
     * one wait carried straight on through the remaining phases, driving services whose caller
     * had already begun stopping them.
     */
    @Test
    void aCancellationBetweenPhasesStopsTheRunAtTheNextBoundary() throws Exception {
        DecommissionCoordinator coordinator = coordinator();
        // Cancelled while OpenSearch's own preparation is in flight — a phase that never looks at
        // isCancelled(). The next boundary is where it has to be caught.
        opensearch.onPrepareDecommission = coordinator::cancel;

        assertThatThrownBy(() -> coordinator.run(null, false))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("cancelled");

        assertThat(calls).containsExactly(
                "opensearch.prepareDecommission",
                // Not prepare-cassandra, not the relocation wait, and above all not either
                // decommission() call.
                "opensearch.abortDecommission");
        assertThat(calls).doesNotContain("cassandra.decommission", "opensearch.decommission");
    }

    /**
     * S4: {@code decommission(opensearch)} belongs inside the compensated block.
     *
     * <p>It is the call that re-checks the shard count and refuses when a shard reappeared between
     * the wait and the re-check, so it genuinely throws — and it throws with this node excluded
     * from shard allocation and Cassandra still fully in the ring. Left uncompensated that is the
     * "every index sits UNASSIGNED" outcome the whole compensation exists to prevent.
     */
    @Test
    void aRefusedOpenSearchDepartureIsStillBackedOut() throws Exception {
        opensearch.decommissionFailure =
                new ServiceException("opensearch", "a shard was allocated back to this node");

        assertThatThrownBy(() -> coordinator().run(null, false))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("decommission-opensearch")
                .hasMessageContaining("a shard was allocated back to this node");

        assertThat(calls).containsExactly(
                "opensearch.prepareDecommission",
                "cassandra.prepareDecommission",
                "opensearch.awaitDecommissionReady",
                "opensearch.decommission",
                // Cassandra never left, so the exclusion must come off again.
                "cassandra.abortDecommission",
                "opensearch.abortDecommission");
        assertThat(calls).doesNotContain("cassandra.decommission");
        assertThat(cassandra.status()).isEqualTo(ServiceStatus.RUNNING);
    }

    /**
     * Compensation is best-effort and must not become the failure the operator reads: the
     * message that says why the decommission stopped is the one worth having.
     */
    @Test
    void aFailureWhileBackingOutDoesNotMaskTheOriginalFailure() throws Exception {
        cassandra.prepareFailure = new ServiceException("cassandra", "this node is the only member of the ring");
        opensearch.abortFailure = new ServiceException("opensearch", "cluster settings update rejected");

        assertThatThrownBy(() -> coordinator().run(null, false))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("only member of the ring")
                .satisfies(thrown -> assertThat(thrown.getSuppressed())
                        .as("the compensation failure is still reported, as a suppressed exception")
                        .anySatisfy(suppressed -> assertThat(suppressed)
                                .hasMessageContaining("cluster settings update rejected")));
    }

    /**
     * Past the point of no return there is nothing to put back. Once Cassandra's
     * {@code decommission()} has begun the ranges are on their way to the rest of the ring, and
     * re-admitting this node to shard allocation would be a lie about a node that is leaving.
     */
    @Test
    void aFailureAfterCassandraHasBegunLeavingIsNotBackedOut() throws Exception {
        cassandra.decommissionFailure = new ServiceException("cassandra", "streaming failed");

        assertThatThrownBy(() -> coordinator().run(null, false))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("streaming failed");

        assertThat(calls).containsExactly(
                "opensearch.prepareDecommission",
                "cassandra.prepareDecommission",
                "opensearch.awaitDecommissionReady",
                "opensearch.decommission",
                "cassandra.decommission");
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
