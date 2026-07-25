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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
        // Opened first: a test that wedged a service must not leave the teardown wedged too.
        openEveryGate();
        if (supervisor != null) {
            supervisor.stop();
        }
    }

    private void openEveryGate() {
        for (RecordingService service : List.of(cassandra, opensearch)) {
            CountDownLatch startGate = service.startGate;
            if (startGate != null) {
                startGate.countDown();
            }
            CountDownLatch stopGate = service.stopGate;
            if (stopGate != null) {
                stopGate.countDown();
            }
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

    /** Startup limits a test can afford to let expire. */
    private static final String IMPATIENT = BOTH.replace("startup_timeout: 5s", "startup_timeout: 300ms");

    /** Shutdown limits a test can afford to let expire, for the abandoned-stop case. */
    private static final String IMPATIENT_SHUTDOWN =
            BOTH.replace("shutdown_timeout: 5s", "shutdown_timeout: 300ms");

    /**
     * Shutdown limits long enough that a test can hold a {@code stop()} open without {@link
     * TimeLimited} giving up on it first — a drain that takes a while is not a drain that failed.
     */
    private static final String WEDGEABLE = BOTH.replace("shutdown_timeout: 5s", "shutdown_timeout: 60s");

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

    /**
     * S1. The one thing a JVM shutdown hook may not do is return early.
     *
     * <p>{@code Shutdown.runHooks()} halts the JVM as soon as every hook has returned. The
     * observed failure: a fatal-error thread wins the shutdown CAS and is minutes into Cassandra's
     * {@code drain()}; an operator sends SIGTERM; the hook loses the CAS and returns in 0 ms; the
     * JVM halts on top of a half-drained node, which then replays its commit log on the next
     * start. "Idempotent" has to mean the work happens once, not that the loser leaves.
     */
    @Test
    void theShutdownHookBlocksUntilAnInFlightShutdownHasFinished() throws Exception {
        Supervisor supervisor = supervisor(WEDGEABLE);
        supervisor.start();
        Thread hook = supervisor.shutdownHook();
        assertThat(hook).isNotNull();

        // Someone else is already inside the ordered shutdown, wedged where a drain would wedge.
        CountDownLatch draining = new CountDownLatch(1);
        cassandra.stopGate = draining;
        Thread firstStopper = new Thread(supervisor::stop, "test-first-stopper");
        firstStopper.start();
        awaitCall("cassandra.stop");

        AtomicReference<SupervisorState> stateWhenHookReturned = new AtomicReference<>();
        AtomicReference<List<String>> callsWhenHookReturned = new AtomicReference<>();
        Thread hookThread = new Thread(() -> {
            hook.run();
            stateWhenHookReturned.set(supervisor.state());
            synchronized (calls) {
                callsWhenHookReturned.set(new ArrayList<>(calls));
            }
        }, "test-jvm-shutdown-hook");
        hookThread.start();

        Thread.sleep(300);
        assertThat(hookThread.isAlive())
                .as("the hook returned while the shutdown was still inside cassandra.stop(); the"
                        + " JVM would have halted on a half-drained node")
                .isTrue();

        draining.countDown();
        hookThread.join(TimeUnit.SECONDS.toMillis(30));
        firstStopper.join(TimeUnit.SECONDS.toMillis(30));

        assertThat(hookThread.isAlive()).isFalse();
        assertThat(stateWhenHookReturned.get()).isEqualTo(SupervisorState.STOPPED);
        assertThat(callsWhenHookReturned.get())
                .as("everything the shutdown had to do was done before the hook returned")
                .containsExactly(
                        "cassandra.start", "opensearch.start",
                        "opensearch.stop", "opensearch.close",
                        "cassandra.stop", "cassandra.close");
    }

    /** The same promise for the public API: stop() returns when the services are stopped. */
    @Test
    void anExplicitStopBlocksUntilAnInFlightShutdownHasFinished() throws Exception {
        Supervisor supervisor = supervisor(WEDGEABLE);
        supervisor.start();

        CountDownLatch draining = new CountDownLatch(1);
        cassandra.stopGate = draining;
        Thread firstStopper = new Thread(supervisor::stop, "test-first-stopper");
        firstStopper.start();
        awaitCall("cassandra.stop");

        Thread secondStopper = new Thread(supervisor::stop, "test-second-stopper");
        secondStopper.start();
        Thread.sleep(300);
        assertThat(secondStopper.isAlive())
                .as("stop() claims to stop every started service; returning here would have"
                        + " stopped none of them")
                .isTrue();

        draining.countDown();
        secondStopper.join(TimeUnit.SECONDS.toMillis(30));
        firstStopper.join(TimeUnit.SECONDS.toMillis(30));
        assertThat(secondStopper.isAlive()).isFalse();
        assertThat(supervisor.state()).isEqualTo(SupervisorState.STOPPED);
    }

    /**
     * A fatal error that arrives while a shutdown is already running is still a fatal error.
     *
     * <p>It loses the shutdown CAS, and the exit code used to stay 0 with a null failure message
     * — telling systemd and the operator that a node which had just lost its commit log directory
     * had stopped on request.
     */
    @Test
    void aFatalErrorDuringAnInFlightShutdownStillReachesTheExitCode() throws Exception {
        Supervisor supervisor = supervisor(WEDGEABLE);
        supervisor.start();

        CountDownLatch draining = new CountDownLatch(1);
        cassandra.stopGate = draining;
        Thread stopper = new Thread(supervisor::stop, "test-stopper");
        stopper.start();
        awaitCall("cassandra.stop");

        cassandra.reportFatalError("commit log directory is unwritable");
        draining.countDown();
        stopper.join(TimeUnit.SECONDS.toMillis(30));

        int exitCode = assertTimeoutPreemptively(Duration.ofSeconds(10), supervisor::awaitShutdown);
        assertThat(exitCode).isEqualTo(1);
        assertThat(supervisor.failureMessage()).contains("commit log directory is unwritable");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.FAILED);
    }

    /**
     * S6. A {@code start()} that overran its deadline is still running, and the unwind must leave
     * it alone.
     *
     * <p>Both runtimes latch {@code stop()} behind a CAS. Stopping an abandoned service during the
     * unwind takes that one stop for a service that is still inside {@code daemon.activate()} —
     * which ignores interrupts — and the late thread then finishes, publishes {@code RUNNING} and
     * starts its watcher threads on top of a supervisor that has already declared it stopped.
     * Nothing can stop those threads afterwards. Closing its ClassLoader is worse: the abandoned
     * thread is executing out of it.
     */
    @Test
    void anAbandonedStartupIsNeitherStoppedNorClosed() throws Exception {
        Supervisor supervisor = supervisor(IMPATIENT);
        // The grace is thirty seconds in production and no test can wait for it.
        supervisor.startupGrace(Duration.ofMillis(100));
        CountDownLatch neverComesUp = new CountDownLatch(1);
        opensearch.startGate = neverComesUp;

        assertThatThrownBy(supervisor::start)
                .isInstanceOf(SupervisorException.class)
                .hasMessageContaining("opensearch")
                .hasMessageContaining("abandoned");

        assertThat(calls)
                .as("the abandoned service must not be stopped or closed; Cassandra still must be")
                .containsExactly(
                        "cassandra.start", "opensearch.start",
                        "cassandra.stop", "cassandra.close");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.FAILED);
        assertThat(supervisor.awaitShutdown()).isEqualTo(1);

        // And when the abandoned startup finally does finish, it finds its stop() unused — which
        // is the whole point: it is still the only thread that could ever stop it.
        neverComesUp.countDown();
        opensearch.startGate = null;
    }

    /**
     * R1. One {@code Throwable} out of a service's {@code stop()} used to wedge the process
     * forever.
     *
     * <p>{@code terminated.countDown()}, {@code removeShutdownHook()} and the final state were all
     * on the straight-line success path, and {@code stopService} caught {@code Exception} — so a
     * {@code NoClassDefFoundError}, which is what a call into a loader that is on its way out
     * actually produces, escaped past all three. The latch stayed closed, so {@code main} never
     * returned and every other caller of {@code stop()} blocked on it for the whole shutdown
     * budget; the shutdown hook stayed registered; the state stayed {@code STOPPING}; and the
     * services after the failing one were never stopped at all.
     */
    @Test
    void anErrorFromOneServiceStopStillTakesTheOthersDownAndLetsTheProcessExit() throws Exception {
        Supervisor supervisor = supervisor(BOTH);
        supervisor.start();
        // Thrown by the service the shutdown reaches first, so "the rest of the walk still
        // happens" is what the assertions below are about.
        opensearch.stopFailure = new NoClassDefFoundError("org/opensearch/Whatever");

        assertTimeoutPreemptively(Duration.ofSeconds(10), supervisor::stop);

        assertThat(calls)
                .as("a service that threw on the way out must not strand the ones behind it")
                .containsExactly(
                        "cassandra.start", "opensearch.start",
                        "opensearch.stop", "opensearch.close",
                        "cassandra.stop", "cassandra.close");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.STOPPED);
        assertThat(assertTimeoutPreemptively(Duration.ofSeconds(5), supervisor::awaitShutdown))
                .as("the latch has to be counted down whatever happened, or main never returns")
                .isZero();
        assertThat(supervisor.shutdownHook())
                .as("and the hook has to be removed, or the JVM runs the whole shutdown again")
                .isNull();
    }

    /**
     * R1, the other half: the final {@code status()} the supervisor records for its own
     * bookkeeping runs <i>after</i> {@code close()} has discarded the loader that answers it.
     *
     * <p>{@code IsolatedService.status()} is hardened against that, but the invariant belongs to
     * the supervisor: an unguarded call here delegates the process's ability to exit to a service
     * implementation, and a {@code null} return was on its own enough to abort the shutdown with
     * an NPE out of {@code ConcurrentHashMap.put}.
     */
    @Test
    void aFinalStatusThatThrowsOrReturnsNullDoesNotAbortTheShutdown() throws Exception {
        Supervisor supervisor = supervisor(BOTH);
        supervisor.start();
        opensearch.nullStatusAfterStop = true;
        cassandra.statusFailureAfterStop = new NoClassDefFoundError("org/apache/cassandra/Whatever");

        assertTimeoutPreemptively(Duration.ofSeconds(10), supervisor::stop);

        assertThat(calls).containsExactly(
                "cassandra.start", "opensearch.start",
                "opensearch.stop", "opensearch.close",
                "cassandra.stop", "cassandra.close");
        assertThat(supervisor.state()).isEqualTo(SupervisorState.STOPPED);
        assertThat(assertTimeoutPreemptively(Duration.ofSeconds(5), supervisor::awaitShutdown)).isZero();
        assertThat(supervisor.shutdownHook()).isNull();
    }

    /**
     * R3. {@code AbandonedException} means the call is <i>still running</i>, and {@code
     * stopService} treated it as an ordinary failure.
     *
     * <p>It is a {@code RuntimeException}, so {@code catch (Exception)} swallowed it and execution
     * fell through to {@code close()} — closing the {@code URLClassLoader} out from under a live
     * {@code drain()} — and then recorded a final status and reported exit code 0 for a service
     * that had never stopped. {@code startService} had honoured the same exception since the day
     * it was introduced; this is the same rule on the shutdown path.
     */
    @Test
    void aStopThatOverranItsDeadlineIsAbandonedRatherThanClosed() throws Exception {
        Supervisor supervisor = supervisor(IMPATIENT_SHUTDOWN);
        supervisor.start();
        CountDownLatch draining = new CountDownLatch(1);
        cassandra.stopGate = draining;
        // Ignores interrupts, like Cassandra's drain(): the deadline expiring does not end the
        // call, it only ends the supervisor's willingness to wait for it.
        cassandra.stopIgnoresInterrupts = true;

        assertTimeoutPreemptively(Duration.ofSeconds(10), supervisor::stop);

        assertThat(cassandra.stopReturned)
                .as("the premise: stop() is still executing inside the service")
                .isFalse();
        assertThat(calls)
                .as("closing its loader now would pull the jars out from under that thread")
                .doesNotContain("cassandra.close");
        assertThat(calls).contains("cassandra.stop");
        assertThat(supervisor.state())
                .as("a service that would not stop is not a clean shutdown")
                .isEqualTo(SupervisorState.FAILED);
        assertThat(assertTimeoutPreemptively(Duration.ofSeconds(5), supervisor::awaitShutdown))
                .isEqualTo(1);
        assertThat(supervisor.failureMessage()).contains("cassandra").contains("abandoned");

        draining.countDown();
    }

    /**
     * R-minor. {@code awaitShutdownInFlight} returned early on an interrupt, which is the one
     * thing it exists to prevent.
     *
     * <p>The caller is usually the JVM's shutdown hook, and an interrupt during shutdown is
     * ordinary — every executor being torn down sends some. Returning on one lets {@code
     * Shutdown.runHooks()} halt the JVM while another thread is inside {@code drain()}, which is
     * exactly the outcome this wait was added to stop.
     */
    @Test
    void aWaitingCallerKeepsWaitingWhenItIsInterrupted() throws Exception {
        Supervisor supervisor = supervisor(WEDGEABLE);
        supervisor.start();
        Thread hook = supervisor.shutdownHook();

        CountDownLatch draining = new CountDownLatch(1);
        cassandra.stopGate = draining;
        Thread firstStopper = new Thread(supervisor::stop, "test-first-stopper");
        firstStopper.start();
        awaitCall("cassandra.stop");

        AtomicReference<Boolean> interruptRestored = new AtomicReference<>();
        Thread hookThread = new Thread(() -> {
            hook.run();
            interruptRestored.set(Thread.currentThread().isInterrupted());
        }, "test-jvm-shutdown-hook");
        hookThread.start();
        Thread.sleep(200);

        hookThread.interrupt();
        Thread.sleep(300);
        assertThat(hookThread.isAlive())
                .as("an interrupt is not permission to halt the JVM on a live shutdown")
                .isTrue();

        draining.countDown();
        hookThread.join(TimeUnit.SECONDS.toMillis(30));
        firstStopper.join(TimeUnit.SECONDS.toMillis(30));

        assertThat(hookThread.isAlive()).isFalse();
        assertThat(supervisor.state()).isEqualTo(SupervisorState.STOPPED);
        assertThat(interruptRestored)
                .as("the interrupt was not obeyed, but it still belongs to that thread")
                .hasValue(true);
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
