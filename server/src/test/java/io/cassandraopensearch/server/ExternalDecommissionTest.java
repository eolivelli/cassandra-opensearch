/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.server.config.NodeConfiguration;
import io.cassandraopensearch.spi.RingStateEvent;
import io.cassandraopensearch.spi.ServiceContext;
import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * What the supervisor does when Cassandra leaves the ring without being asked.
 *
 * <p>The event that says so is the same event the supported path produces, so most of what is
 * asserted here is <i>restraint</i>: firing during {@code bin/cassandra-opensearch decommission}
 * would run a second exclusion against the OpenSearch node the coordinator is already driving.
 * The one case that must act is a ring event arriving while the supervisor believes nothing is
 * happening.
 */
class ExternalDecommissionTest {

    /**
     * How long a test waits before concluding that nothing is going to happen. The guard that
     * decides whether to react runs inline on the reporting thread, so a decision not to react
     * has already been taken by the time {@code reportEvent} returns; this only covers the thread
     * that a decision to react would have started.
     */
    private static final Duration SETTLE = Duration.ofMillis(500);

    @TempDir
    Path home;

    private final List<String> calls = new ArrayList<>();
    private final RecordingService cassandra = new RecordingService("cassandra", calls);
    private final RecordingService opensearch = new RecordingService("opensearch", calls);

    private Supervisor supervisor;

    @AfterEach
    void stopSupervisor() {
        opensearch.prepareGate = null;
        opensearch.relocationGate = null;
        if (supervisor != null) {
            supervisor.stop();
        }
    }

    private static final String WATCHING = """
            services:
              cassandra:
                startup_timeout: 5s
                shutdown_timeout: 5s
              opensearch:
                startup_timeout: 5s
                shutdown_timeout: 5s
            decommission:
              shard_relocation_timeout: 30s
              watch_external_decommission: true
            supervisor:
              health_check_interval: 50ms
            """;

    private static final String NOT_WATCHING = WATCHING.replace(
            "watch_external_decommission: true", "watch_external_decommission: false");

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

    /** What {@code RingStateWatcher} reports when it sees a bare {@code nodetool decommission}. */
    private void reportLeaving() {
        cassandra.context().reportEvent("WARN", RingStateEvent.TYPE,
                new RingStateEvent("LEAVING", true, "operation mode moved NORMAL -> LEAVING").toMessage());
    }

    @Test
    void excludesTheOpenSearchNodeWhenCassandraLeavesTheRingUnasked() throws Exception {
        Supervisor supervisor = supervisor(WATCHING);
        supervisor.start();

        reportLeaving();

        awaitCall("opensearch.prepareDecommission");
        // Only the exclusion. Nothing here drives Cassandra — it is already leaving — and nothing
        // waits for the relocation it started; see the class comment on Supervisor.
        assertThat(calls).doesNotContain("cassandra.prepareDecommission", "opensearch.decommission");
        assertThat(supervisor.state())
                .as("the supervisor did not ask for this and must not start behaving as if it had")
                .isEqualTo(SupervisorState.RUNNING);
        assertThat(cassandra.status()).isEqualTo(ServiceStatus.RUNNING);
    }

    @Test
    void doesNothingWhileTheSupervisorIsDrivingItsOwnDecommission() throws Exception {
        Supervisor supervisor = supervisor(WATCHING);
        supervisor.start();

        // Hold the coordinator inside the shard-relocation wait: that is precisely the window in
        // which the real Cassandra reports LEAVING for the supervisor's own decommission.
        CountDownLatch relocating = new CountDownLatch(1);
        opensearch.relocationGate = relocating;
        Thread coordinated = new Thread(() -> {
            try {
                supervisor.decommission(Duration.ofSeconds(30), true);
            } catch (DecommissionException e) {
                throw new AssertionError(e);
            }
        }, "test-coordinated-decommission");
        coordinated.start();
        awaitCall("opensearch.awaitDecommissionReady");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.DECOMMISSIONING);

        reportLeaving();
        Thread.sleep(SETTLE.toMillis());

        assertThat(calls).filteredOn("opensearch.prepareDecommission"::equals)
                .as("the coordinator's exclusion is the only one there may be")
                .hasSize(1);

        relocating.countDown();
        opensearch.relocationGate = null;
        coordinated.join(TimeUnit.MINUTES.toMillis(1));
        assertThat(calls).filteredOn("opensearch.prepareDecommission"::equals).hasSize(1);
    }

    @Test
    void doesNothingWhenTheSettingIsOff() throws Exception {
        Supervisor supervisor = supervisor(NOT_WATCHING);
        supervisor.start();

        reportLeaving();
        Thread.sleep(SETTLE.toMillis());

        assertThat(calls).containsExactly("cassandra.start", "opensearch.start");
    }

    @Test
    void reactsOnlyOnce() throws Exception {
        Supervisor supervisor = supervisor(WATCHING);
        supervisor.start();

        // A real watcher reports LEAVING and then DECOMMISSIONED. The second is not a second
        // decommission, and excluding an already-excluded node again is a pointless cluster-state
        // update on a node that is on its way out.
        reportLeaving();
        awaitCall("opensearch.prepareDecommission");
        cassandra.context().reportEvent("WARN", RingStateEvent.TYPE,
                new RingStateEvent("DECOMMISSIONED", true, "moved LEAVING -> DECOMMISSIONED").toMessage());
        Thread.sleep(SETTLE.toMillis());

        assertThat(calls).filteredOn("opensearch.prepareDecommission"::equals).hasSize(1);
    }

    @Test
    void ignoresRingEventsThatDoNotSayTheNodeIsLeaving() throws Exception {
        Supervisor supervisor = supervisor(WATCHING);
        supervisor.start();

        // MOVING and DRAINING both come through this event. Excluding a node that is moving a
        // token would relocate every shard off it for nothing.
        cassandra.context().reportEvent("INFO", RingStateEvent.TYPE,
                new RingStateEvent("MOVING", false, "moved NORMAL -> MOVING").toMessage());
        // And a message that is not in the grammar at all must not be guessed at.
        cassandra.context().reportEvent("WARN", RingStateEvent.TYPE, "the node is leaving, honestly");
        Thread.sleep(SETTLE.toMillis());

        assertThat(calls).containsExactly("cassandra.start", "opensearch.start");
    }

    @Test
    void doesNotBlockTheThreadThatReportedTheEvent() throws Exception {
        Supervisor supervisor = supervisor(WATCHING);
        supervisor.start();

        // reportEvent is called from inside the service, often on a Cassandra thread, and the SPI
        // says it never blocks. Here the exclusion itself is wedged for as long as the test likes.
        CountDownLatch excluding = new CountDownLatch(1);
        opensearch.prepareGate = excluding;

        long start = System.nanoTime();
        reportLeaving();
        Duration reporting = Duration.ofNanos(System.nanoTime() - start);

        assertThat(reporting).isLessThan(Duration.ofSeconds(1));
        awaitCall("opensearch.prepareDecommission");
        assertThat(excluding.getCount())
                .as("the exclusion must still be in flight, on a thread that is not the reporter's")
                .isEqualTo(1);
        excluding.countDown();
        opensearch.prepareGate = null;
    }

    @Test
    void aFailedExclusionLeavesBothServicesAlone() throws Exception {
        opensearch.prepareFailure = new ServiceException("opensearch", "no cluster manager");
        Supervisor supervisor = supervisor(WATCHING);
        supervisor.start();

        reportLeaving();
        awaitCall("opensearch.prepareDecommission");
        Thread.sleep(SETTLE.toMillis());

        // Best effort means best effort: a failure is the operator's problem to fix by hand, not
        // grounds for the supervisor to take a node down that is still serving.
        assertThat(supervisor.state()).isEqualTo(SupervisorState.RUNNING);
        assertThat(cassandra.status()).isEqualTo(ServiceStatus.RUNNING);
        assertThat(calls).containsExactly(
                "cassandra.start", "opensearch.start", "opensearch.prepareDecommission");
        assertThat(supervisor.failureMessage()).isNull();
    }

    /**
     * The listener runs supervisor code on a thread belonging to the service, and the SPI promises
     * {@code reportEvent} never throws. A defect on our side of that call must therefore not
     * surface inside Cassandra as a failure of whatever it was doing at the time.
     */
    @Test
    void aListenerThatThrowsCannotReachTheService() throws Exception {
        Files.createDirectories(home.resolve("conf"));
        Files.writeString(home.resolve("conf").resolve("cassandra-opensearch.yaml"), WATCHING);
        NodeConfiguration configuration = NodeConfiguration.load(home);

        ServiceContext context = new SupervisorServiceContext(
                home, configuration.service("cassandra"), Map.of(),
                (service, level, type, message) -> {
                    throw new IllegalStateException("a defect in the supervisor's own handler");
                },
                (message, cause) -> {
                });

        assertThatCode(() -> context.reportEvent("WARN", RingStateEvent.TYPE,
                new RingStateEvent("LEAVING", true, "").toMessage()))
                .doesNotThrowAnyException();
    }

    @Test
    void theEventGrammarSurvivesARoundTrip() {
        RingStateEvent event = new RingStateEvent("LEAVING", true, "moved NORMAL -> LEAVING");

        assertThat(RingStateEvent.parse(event.toMessage())).isEqualTo(event);
        assertThat(RingStateEvent.parse(new RingStateEvent("MOVING", false, "").toMessage()))
                .isEqualTo(new RingStateEvent("MOVING", false, ""));
        assertThat(RingStateEvent.parse("this node has been decommissioned")).isNull();
        assertThat(RingStateEvent.parse("state=LEAVING; no verdict")).isNull();
        assertThat(RingStateEvent.parse(null)).isNull();
    }

    private void awaitCall(String call) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (true) {
            synchronized (calls) {
                if (calls.contains(call)) {
                    return;
                }
            }
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("timed out waiting for " + call + "; saw " + calls);
            }
            Thread.sleep(20);
        }
    }
}
