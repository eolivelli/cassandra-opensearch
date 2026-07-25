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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What happens when a decommission is not the only thing going on.
 *
 * <p>Both cases here were found the same way: the decommission is a JMX call that blocks for as
 * long as the ring takes to hand over — an hour is normal — and prints nothing while it does. That
 * is precisely the situation in which an operator opens a second terminal, or gives up and sends
 * SIGTERM. Neither used to be safe.
 */
class SupervisorDecommissionConcurrencyTest {

    @TempDir
    Path home;

    private final List<String> calls = Collections.synchronizedList(new ArrayList<>());
    private final RecordingService cassandra = new RecordingService("cassandra", calls);
    private final RecordingService opensearch = new RecordingService("opensearch", calls);

    private Supervisor supervisor;

    private static final String CONFIGURATION = """
            services:
              cassandra:
                startup_timeout: 5s
                shutdown_timeout: 10s
              opensearch:
                startup_timeout: 5s
                shutdown_timeout: 10s
            decommission:
              shard_relocation_timeout: 60s
              ring_streaming_timeout: 60s
            supervisor:
              health_check_interval: 50ms
            """;

    @AfterEach
    void stopSupervisor() {
        // Every gate first: a test that wedged a service must not leave the teardown wedged too.
        for (RecordingService service : List.of(cassandra, opensearch)) {
            for (CountDownLatch gate : new CountDownLatch[] {
                    service.relocationGate, service.decommissionGate, service.statusGate}) {
                if (gate != null) {
                    gate.countDown();
                }
            }
        }
        if (supervisor != null) {
            supervisor.stop();
        }
    }

    private Supervisor supervisor() throws IOException {
        Files.createDirectories(home.resolve("conf"));
        Files.writeString(home.resolve("conf").resolve("cassandra-opensearch.yaml"), CONFIGURATION);
        supervisor = new Supervisor(NodeConfiguration.load(home), service -> switch (service.name()) {
            case "cassandra" -> cassandra;
            case "opensearch" -> opensearch;
            default -> throw new IllegalArgumentException(service.name());
        });
        return supervisor;
    }

    /**
     * S2. Four operators, one node.
     *
     * <p>{@code decommission()} read {@code state} and then installed a coordinator with nothing
     * in between, so two callers who both saw {@code RUNNING} both got one — two coordinators
     * driving the same two services, and two concurrent {@code StorageService.decommission()}
     * calls, which the SPI says explicitly must never happen. {@code decommissionInFlight} was
     * clobbered as well, and nulled by whichever finished first.
     */
    @Test
    void onlyOneDecommissionMayEverBeInFlight() throws Exception {
        Supervisor supervisor = supervisor();
        supervisor.start();
        // The winner parks in the shard-relocation wait, which is where a real decommission
        // spends nearly all of its time, so every other caller is genuinely concurrent with it.
        CountDownLatch relocating = new CountDownLatch(1);
        opensearch.relocationGate = relocating;

        int callers = 4;
        CyclicBarrier together = new CyclicBarrier(callers);
        AtomicInteger accepted = new AtomicInteger();
        List<String> refusals = Collections.synchronizedList(new ArrayList<>());
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            Thread thread = new Thread(() -> {
                try {
                    together.await(30, TimeUnit.SECONDS);
                    supervisor.decommission(null, true);
                    accepted.incrementAndGet();
                } catch (DecommissionException e) {
                    refusals.add(e.getMessage());
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }, "test-decommission-" + i);
            threads.add(thread);
            thread.start();
        }

        awaitRefusals(refusals, callers - 1);
        assertThat(refusals)
                .as("the losers must be told a decommission is already running, not left to start"
                        + " a second coordinator over the same two services")
                .allSatisfy(message -> assertThat(message).contains("already in flight"));

        relocating.countDown();
        opensearch.relocationGate = null;
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(60));
        }

        assertThat(accepted.get()).isEqualTo(1);
        assertThat(calls).filteredOn("cassandra.decommission"::equals)
                .as("StorageService.decommission() may be called exactly once")
                .hasSize(1);
        assertThat(calls).filteredOn("opensearch.prepareDecommission"::equals).hasSize(1);
    }

    /** The same refusal through the ordinary sequential path, with its message. */
    @Test
    void aSecondDecommissionIsRefusedWhileTheFirstIsWaiting() throws Exception {
        Supervisor supervisor = supervisor();
        supervisor.start();
        CountDownLatch relocating = new CountDownLatch(1);
        opensearch.relocationGate = relocating;

        Thread first = new Thread(() -> {
            try {
                supervisor.decommission(null, true);
            } catch (DecommissionException e) {
                throw new AssertionError(e);
            }
        }, "test-first-decommission");
        first.start();
        awaitCall("opensearch.awaitDecommissionReady");

        assertThatThrownBy(() -> supervisor.decommission(null, false))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("already in flight");

        relocating.countDown();
        opensearch.relocationGate = null;
        first.join(TimeUnit.SECONDS.toMillis(60));
    }

    /**
     * S3. SIGTERM in the middle of a decommission.
     *
     * <p>{@code cancel()} only set a volatile boolean that the coordinator never read, so the
     * shutdown ran to completion — both services stopped, {@code IsolatedService.close()} called,
     * the {@code URLClassLoader}s closed — and the coordinator then went on to call
     * {@code decommission(cassandra)}, streaming ranges through closed jars. A ring transition
     * begun and abandoned, moments before the JVM halted.
     *
     * <p>So the shutdown has to cancel the coordinator and <i>wait for it</i> before it touches a
     * service, and the coordinator has to notice at a phase boundary.
     */
    @Test
    void aShutdownDuringADecommissionWaitsForTheCoordinatorToUnwind() throws Exception {
        Supervisor supervisor = supervisor();
        supervisor.start();
        CountDownLatch relocating = new CountDownLatch(1);
        opensearch.relocationGate = relocating;

        List<Throwable> outcome = Collections.synchronizedList(new ArrayList<>());
        Thread decommissioning = new Thread(() -> {
            try {
                supervisor.decommission(null, false);
            } catch (Throwable e) {
                outcome.add(e);
            }
        }, "test-decommission");
        decommissioning.start();
        awaitCall("opensearch.awaitDecommissionReady");

        // What the JVM does on SIGTERM. Never opens the relocation gate: the coordinator has to
        // get out through the cancellation, not because the wait happened to finish.
        Thread hook = supervisor.shutdownHook();
        hook.run();
        decommissioning.join(TimeUnit.SECONDS.toMillis(60));

        assertThat(outcome).hasSize(1);
        assertThat(outcome.get(0))
                .isInstanceOf(DecommissionException.class)
                .hasMessageContaining("cancelled");
        assertThat(calls)
                .as("the ring transition must never have begun")
                .doesNotContain("cassandra.decommission", "opensearch.decommission");
        // Cancelled short of the point of no return, so the node is put back before it is stopped:
        // both aborts, then the ordered shutdown, and the loaders are closed last of all.
        assertThat(calls).containsExactly(
                "cassandra.start", "opensearch.start",
                "opensearch.prepareDecommission", "cassandra.prepareDecommission",
                "opensearch.awaitDecommissionReady",
                "cassandra.abortDecommission", "opensearch.abortDecommission",
                "opensearch.stop", "opensearch.close",
                "cassandra.stop", "cassandra.close");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.STOPPED);
    }

    /**
     * R2. The lock the round-1 fix introduced, and the three-thread jam it made possible.
     *
     * <p>{@code checkHealth} held the {@code started} monitor across every {@code status()} call
     * into an isolated loader — {@code synchronizedMap.forEach} holds it anyway — and neither
     * runtime's {@code status()} is interruptible, so {@code stopHealthMonitor()} cannot free one
     * that has wedged. {@code beginDecommission} then wanted that same monitor <i>while holding
     * {@code transitionLock}</i>, and the shutdown wanted {@code transitionLock} before it reached
     * the line that stops the health monitor. Health thread wedged in {@code status()},
     * decommissioner blocked behind it holding {@code transitionLock}, shutdown blocked on
     * {@code transitionLock}: no deadline anywhere, and the process never went down.
     *
     * <p>The rule this pins is "no monitor is held across a call into a service", and both halves
     * of it are needed: the health poll must not hold one, and the transition must not want one.
     */
    @Test
    void aWedgedStatusCallBlocksNeitherADecommissionNorAShutdown() throws Exception {
        Supervisor supervisor = supervisor();
        supervisor.start();
        // A status() that has stopped answering, on the thread that polls it every 50ms.
        CountDownLatch wedged = new CountDownLatch(1);
        opensearch.statusGate = wedged;
        assertThat(opensearch.statusWedged.await(30, TimeUnit.SECONDS))
                .as("the premise: the health monitor is inside a status() that will not return")
                .isTrue();
        // And a decommission that parks where a real one spends its time.
        CountDownLatch relocating = new CountDownLatch(1);
        opensearch.relocationGate = relocating;

        List<Throwable> outcome = Collections.synchronizedList(new ArrayList<>());
        Thread decommissioning = new Thread(() -> {
            try {
                supervisor.decommission(null, false);
            } catch (Throwable e) {
                outcome.add(e);
            }
        }, "test-decommission");
        decommissioning.start();
        // This is where it used to stop: blocked at `synchronized (started)` inside
        // beginDecommission, holding transitionLock.
        awaitCall("opensearch.awaitDecommissionReady");

        Thread stopper = new Thread(supervisor::stop, "test-stopper");
        stopper.start();
        // And this is where the shutdown used to stop: blocked on transitionLock, which it takes
        // before the line that would have stopped the health monitor.
        awaitCall("opensearch.stop");

        wedged.countDown();
        opensearch.statusGate = null;
        stopper.join(TimeUnit.SECONDS.toMillis(60));
        decommissioning.join(TimeUnit.SECONDS.toMillis(60));

        assertThat(stopper.isAlive()).isFalse();
        assertThat(outcome).singleElement().isInstanceOf(DecommissionException.class);
        assertThat(calls).contains("cassandra.stop", "cassandra.close");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.STOPPED);
    }

    /**
     * R4. The phase a cancellation cannot reach, which is the phase the whole cancellation
     * mechanism was documented as covering.
     *
     * <p>{@code decommission-cassandra} sits past the last {@code checkNotCancelled()}, is bounded
     * by {@code ring_streaming_timeout} plus a grace rather than by the cancellation budget, and
     * {@code StorageService.decommission()} neither polls nor honours an interrupt. A shutdown
     * arriving there waits out its budget and then used to stop and close both services anyway —
     * closing the loader while ranges were still streaming through it, and reporting exit 0. The
     * round-1 test cancelled inside {@code awaitDecommissionReady}, the one phase that does honour
     * cancellation, so it passed throughout.
     *
     * <p>What has to happen instead is what happens to any call that is still running when the
     * supervisor gives up on it: the services are abandoned. The coordinator stops them itself
     * when it finally returns, and that is the only stop they may get.
     */
    @Test
    void aShutdownDuringTheUncancellablePhaseAbandonsTheServicesRatherThanClosingThem()
            throws Exception {
        Supervisor supervisor = supervisor();
        // A minute in production; the test asserts what happens after it expires, not how long
        // it is.
        supervisor.decommissionCancelBudget(Duration.ofMillis(300));
        supervisor.start();
        CountDownLatch streaming = new CountDownLatch(1);
        cassandra.decommissionGate = streaming;

        List<Throwable> outcome = Collections.synchronizedList(new ArrayList<>());
        Thread decommissioning = new Thread(() -> {
            try {
                supervisor.decommission(null, false);
            } catch (Throwable e) {
                outcome.add(e);
            }
        }, "test-decommission");
        decommissioning.start();
        awaitCall("cassandra.decommission");

        // SIGTERM, arriving while Cassandra is streaming its ranges away.
        Thread hook = supervisor.shutdownHook();
        hook.run();

        assertThat(streaming.getCount())
                .as("the premise: the decommission is still inside the phase nothing can cancel")
                .isEqualTo(1);
        assertThat(calls)
                .as("stopping takes the one stop() the coordinator still needs; closing pulls the"
                        + " jars out from under a live decommission()")
                .doesNotContain("cassandra.stop", "cassandra.close",
                        "opensearch.stop", "opensearch.close");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.FAILED);
        assertThat(supervisor.awaitShutdown())
                .as("a node left mid-decommission has not stopped cleanly and must not say it did")
                .isEqualTo(1);
        assertThat(supervisor.failureMessage()).contains("abandoned");

        streaming.countDown();
        cassandra.decommissionGate = null;
        decommissioning.join(TimeUnit.SECONDS.toMillis(60));
        assertThat(outcome).isEmpty();
        assertThat(calls)
                .as("and the coordinator is still the one that stops them, when it gets there")
                .contains("opensearch.stop", "cassandra.stop");
    }

    private void awaitRefusals(List<String> refusals, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (refusals.size() < expected) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("expected " + expected + " refusals, saw " + refusals);
            }
            Thread.sleep(20);
        }
    }

    private void awaitCall(String call) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!calls.contains(call)) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("timed out waiting for " + call + "; saw " + calls);
            }
            Thread.sleep(20);
        }
    }
}
