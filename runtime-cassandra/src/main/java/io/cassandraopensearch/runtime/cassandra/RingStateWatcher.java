/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.time.Duration;
import java.util.Set;
import java.util.function.Supplier;

import io.cassandraopensearch.spi.RingStateEvent;
import io.cassandraopensearch.spi.ServiceContext;

/**
 * Reports Cassandra's operation mode to the supervisor whenever it moves.
 *
 * <p>This exists for the one transition the supervisor cannot cause and cannot otherwise see: an
 * operator running {@code bin/nodetool decommission}, which talks straight to the {@code
 * StorageService} MBean and drives the node into {@code LEAVING} without the supervisor being
 * involved at all. Without a watcher, the first the rest of the process learns of it is when it
 * dies.
 *
 * <h2>Why polling rather than a JMX notification</h2>
 *
 * {@code StorageService} does extend {@code NotificationBroadcasterSupport}, so a {@link
 * javax.management.NotificationListener} on {@code StorageServiceMBean} looks like the tidier
 * option. It would observe nothing. The only notifications that class emits come from its {@code
 * JMXProgressSupport}, which is wired to bootstrap-resume and repair tasks; {@code setMode} — the
 * single method that assigns {@code operationMode}, and the one every transition in the class
 * goes through — writes the field and logs, and broadcasts nothing. There is no event to
 * subscribe to, so the mode has to be read.
 *
 * <p>A one-second poll is comfortably fast enough for what it is watching. {@code
 * StorageService.decommission} sets {@code LEAVING} and then sleeps {@code RING_DELAY_MILLIS}
 * (30 s by default) before it streams anything, so the interesting state is on display for tens
 * of seconds. {@code DECOMMISSIONED} is watched for as well, as the backstop for a poll that
 * straddles the whole of {@code LEAVING} anyway — on a node that was already leaving when the
 * watcher started, for instance.
 *
 * <h2>Thread</h2>
 *
 * One daemon thread. It pins the isolated ClassLoader while it runs, which is not a new problem —
 * about eleven of Cassandra's own threads already do, two of them unfixably (see
 * {@code docs/KNOWN-GAPS.md} §4) — but it must never hold the JVM open, hence daemon, and it must
 * be gone before the supervisor discards the loader, hence the join in {@link #stop()}.
 */
final class RingStateWatcher {

    private static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(1);

    /**
     * Operation modes that mean this node is leaving the ring for good.
     *
     * <p>Deliberately not {@code MOVING} or {@code DRAINING}: a node that is moving a token or
     * being drained is still a ring member and still owns its data, and excluding its OpenSearch
     * half from shard allocation would relocate every shard away for nothing. {@code
     * DECOMMISSION_FAILED} is excluded for the opposite reason — the node did not leave.
     */
    private static final Set<String> LEAVING_THE_RING = Set.of("LEAVING", "DECOMMISSIONED");

    private final ServiceContext context;
    private final Supplier<String> operationMode;
    private final Duration interval;
    private final Thread thread;

    private volatile boolean running = true;

    /**
     * The mode the last poll saw. Set once here and thereafter touched only by {@link #thread}, so
     * no transition can be reported twice or missed; {@code Thread.start()} publishes it safely.
     *
     * <p>Taking the baseline in the constructor rather than in the first poll matters: it is the
     * mode at the moment the watch was asked for, not the mode at whatever later moment the new
     * thread happened to be scheduled, and a transition in between must be reported rather than
     * absorbed.
     */
    private String lastSeen;

    private RingStateWatcher(ServiceContext context, Supplier<String> operationMode, Duration interval) {
        this.context = context;
        this.operationMode = operationMode;
        this.interval = interval;
        this.lastSeen = read();
        this.thread = new Thread(this::run, "cassandra-ring-state-watcher");
        this.thread.setDaemon(true);
    }

    static RingStateWatcher start(ServiceContext context, Supplier<String> operationMode) {
        return start(context, operationMode, DEFAULT_INTERVAL);
    }

    static RingStateWatcher start(ServiceContext context, Supplier<String> operationMode, Duration interval) {
        RingStateWatcher watcher = new RingStateWatcher(context, operationMode, interval);
        watcher.thread.start();
        return watcher;
    }

    /**
     * Stops watching and waits for the thread to end.
     *
     * <p>The wait is not politeness. {@code EmbeddedService.stop()} promises that no
     * implementation code is still running when it returns, because the supervisor discards the
     * isolated ClassLoader immediately afterwards.
     */
    void stop() {
        running = false;
        thread.interrupt();
        try {
            thread.join(interval.plusSeconds(5).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void run() {
        while (running) {
            if (!sleep()) {
                return;
            }
            String mode = read();
            if (mode == null || mode.equals(lastSeen)) {
                continue;
            }
            String previous = lastSeen;
            lastSeen = mode;
            report(previous, mode);
        }
    }

    private void report(String previous, String mode) {
        boolean leaving = LEAVING_THE_RING.contains(mode);
        RingStateEvent event = new RingStateEvent(mode, leaving,
                "Cassandra's operation mode moved " + previous + " -> " + mode);
        context.reportEvent(leaving ? "WARN" : "INFO", RingStateEvent.TYPE, event.toMessage());
    }

    /**
     * @return the current operation mode, or null when it cannot be read. A node being torn down
     *         can throw from anywhere in {@code StorageService}, and a watcher that died of it
     *         would take the ring reporting with it for the rest of the process's life.
     */
    private String read() {
        try {
            return operationMode.get();
        } catch (Throwable t) {
            return null;
        }
    }

    /** @return false if the wait was cut short by {@link #stop()} */
    private boolean sleep() {
        try {
            Thread.sleep(interval.toMillis());
            return running;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
