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
        CountDownLatch relocating = opensearch.relocationGate;
        if (relocating != null) {
            relocating.countDown();
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
