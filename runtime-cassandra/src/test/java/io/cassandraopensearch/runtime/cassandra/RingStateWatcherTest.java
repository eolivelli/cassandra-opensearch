/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.cassandraopensearch.spi.RingStateEvent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The watcher on its own, against a mode it can be told to change.
 *
 * <p>No node here on purpose. What is being asserted is the watcher's own arithmetic — which
 * transitions it reports, which it stays quiet about, and that its thread is gone when it says it
 * is — and a real Cassandra offers no way to walk it through a mode sequence on demand.
 * {@code CassandraServiceLifecycleTest} covers the other half: that a real node's operation mode
 * is what the watcher is actually reading.
 */
class RingStateWatcherTest {

    /** Short enough that a test is not a wait, long enough that a loaded machine keeps up. */
    private static final Duration INTERVAL = Duration.ofMillis(50);

    private static final String THREAD_NAME = "cassandra-ring-state-watcher";

    @TempDir
    Path home;

    private final AtomicReference<String> mode = new AtomicReference<>("NORMAL");

    private RecordingServiceContext context;
    private RingStateWatcher watcher;

    @AfterEach
    void stopWatcher() {
        if (watcher != null) {
            watcher.stop();
            watcher = null;
        }
    }

    private RingStateWatcher watching() {
        context = new RecordingServiceContext(home, Map.of());
        watcher = RingStateWatcher.start(context, mode::get, INTERVAL);
        return watcher;
    }

    @Test
    void reportsAMoveIntoLeavingAsSomethingTheNodeIsDoingWithoutBeingAsked() throws Exception {
        watching();

        mode.set("LEAVING");

        RingStateEvent event = awaitRingEvent();
        assertThat(event.state()).isEqualTo("LEAVING");
        assertThat(event.leaving())
                .as("the runtime owns the mode vocabulary and has to state the conclusion")
                .isTrue();
        assertThat(event.detail()).contains("NORMAL").contains("LEAVING");
        assertThat(ringEvents().get(0).level())
                .as("a ring this node is leaving is not routine information")
                .isEqualTo("WARN");
    }

    @Test
    void reportsDecommissionedToo() throws Exception {
        watching();

        // A poll can straddle the whole of LEAVING — on a node that was already leaving when the
        // watcher started, for instance — so the terminal mode has to count as well.
        mode.set("DECOMMISSIONED");

        RingStateEvent event = awaitRingEvent();
        assertThat(event.state()).isEqualTo("DECOMMISSIONED");
        assertThat(event.leaving()).isTrue();
    }

    @Test
    void reportsAMoveThatIsNotALeavingAsInformationOnly() throws Exception {
        watching();

        mode.set("MOVING");

        RingStateEvent event = awaitRingEvent();
        assertThat(event.state()).isEqualTo("MOVING");
        assertThat(event.leaving())
                .as("a node moving a token still owns its data; excluding it would be wrong")
                .isFalse();
        assertThat(ringEvents().get(0).level()).isEqualTo("INFO");
    }

    /**
     * {@code DECOMMISSION_FAILED} says a decommission ended badly. It does not say the node left.
     *
     * <p>The fork sets that mode from catch blocks around the whole of
     * {@code StorageService.decommission()}, and the ordinary way to reach it is a refusal
     * evaluated before anything moves — an unforced decommission that would drop
     * {@code system_distributed} below RF 3, which is every cluster of fewer than four nodes. The
     * node is then a full ring member with the mode string as the only trace, and reporting a
     * departure would have the supervisor exclude its OpenSearch half from shard allocation and
     * relocate every shard off a node that is not going anywhere.
     */
    @Test
    void reportsAFailedDecommissionAsInformationOnly() throws Exception {
        watching();

        mode.set("DECOMMISSION_FAILED");

        RingStateEvent event = awaitRingEvent();
        assertThat(event.state()).isEqualTo("DECOMMISSION_FAILED");
        assertThat(event.leaving())
                .as("the mode does not say whether leaveRing() ran; CassandraService asks"
                        + " TokenMetadata instead")
                .isFalse();
        assertThat(ringEvents().get(0).level()).isEqualTo("INFO");
    }

    @Test
    void saysNothingWhileTheModeDoesNotMove() throws Exception {
        watching();

        Thread.sleep(INTERVAL.multipliedBy(10).toMillis());

        assertThat(ringEvents())
                .as("the mode the watcher starts on is a baseline, not a transition")
                .isEmpty();
    }

    @Test
    void reportsEachTransitionOnceAndInOrder() throws Exception {
        watching();

        mode.set("LEAVING");
        awaitRingEvent();
        mode.set("DECOMMISSIONED");
        awaitEvents(2);
        Thread.sleep(INTERVAL.multipliedBy(6).toMillis());

        assertThat(ringEvents().stream().map(event -> RingStateEvent.parse(event.message()).state()))
                .containsExactly("LEAVING", "DECOMMISSIONED");
    }

    @Test
    void keepsWatchingWhenTheModeCannotBeRead() throws Exception {
        context = new RecordingServiceContext(home, Map.of());
        watcher = RingStateWatcher.start(context, () -> {
            String current = mode.get();
            if (current == null) {
                // A node being torn down throws from anywhere in StorageService. Losing the watch
                // for the rest of the process's life over one such read would be the wrong trade.
                throw new IllegalStateException("the node is on its way down");
            }
            return current;
        }, INTERVAL);

        mode.set(null);
        Thread.sleep(INTERVAL.multipliedBy(4).toMillis());
        mode.set("LEAVING");

        assertThat(awaitRingEvent().state()).isEqualTo("LEAVING");
    }

    /**
     * The thread must not hold the JVM open, and must be gone before the supervisor discards the
     * isolated ClassLoader — which it does the moment {@code stop()} returns.
     */
    @Test
    void itsThreadIsADaemonAndIsGoneAfterStop() {
        watching();

        Thread thread = watcherThread();
        assertThat(thread).isNotNull();
        assertThat(thread.isDaemon()).isTrue();

        watcher.stop();
        watcher = null;

        assertThat(watcherThread()).isNull();
    }

    /**
     * The join in {@link RingStateWatcher#stop()} can expire, and when it does there is nothing
     * left to try: the caller discards the isolated ClassLoader the moment {@code stop()} returns,
     * so a watcher still running is a leaked loader and a thread reporting into a service that no
     * longer exists. Swallowing that silently makes it invisible; it has to be said out loud.
     */
    @Test
    void saysSoWhenItsThreadWillNotStop() throws Exception {
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        context = new RecordingServiceContext(home, Map.of());
        watcher = RingStateWatcher.start(context, () -> {
            if (reads.getAndIncrement() == 0) {
                // The baseline the constructor takes, on the caller's thread.
                return "NORMAL";
            }
            reading.countDown();
            // Deliberately not interruptible: a read wedged inside a node being torn down is what
            // this is modelling, and stop()'s interrupt does not free it.
            while (true) {
                try {
                    release.await();
                    return "NORMAL";
                } catch (InterruptedException ignored) {
                    // Keep waiting, exactly as a wedged read would.
                }
            }
        }, INTERVAL);
        assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();

        try {
            watcher.stop();

            assertThat(context.events())
                    .filteredOn(event -> event.type().equals("cassandra.ring.watcher.stuck"))
                    .singleElement()
                    .satisfies(event -> {
                        assertThat(event.level()).isEqualTo("WARN");
                        assertThat(event.message()).contains("ClassLoader");
                    });
        } finally {
            release.countDown();
            watcher.stop();
            watcher = null;
        }
        assertThat(watcherThread()).isNull();
    }

    // --- helpers ---------------------------------------------------------------------------

    private static Thread watcherThread() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> THREAD_NAME.equals(thread.getName()) && thread.isAlive())
                .findFirst()
                .orElse(null);
    }

    private List<RecordingServiceContext.Event> ringEvents() {
        return context.events().stream()
                .filter(event -> RingStateEvent.TYPE.equals(event.type()))
                .toList();
    }

    private RingStateEvent awaitRingEvent() throws InterruptedException {
        awaitEvents(1);
        return RingStateEvent.parse(ringEvents().get(0).message());
    }

    private void awaitEvents(int count) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (ringEvents().size() < count) {
            if (System.nanoTime() - deadline > 0) {
                throw new AssertionError("only " + ringEvents().size() + " of " + count
                        + " ring events arrived: " + context.events());
            }
            Thread.sleep(10);
        }
    }
}
