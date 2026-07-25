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
