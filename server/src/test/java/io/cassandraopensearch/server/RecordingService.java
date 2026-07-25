/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.spi.DecommissionContext;
import io.cassandraopensearch.spi.EmbeddedService;
import io.cassandraopensearch.spi.ServiceContext;
import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stand-in for one embedded server that records every lifecycle call into a shared list.
 *
 * <p>The supervisor's contract is about <i>order</i> — Cassandra before OpenSearch, OpenSearch
 * out before Cassandra leaves the ring — and order is what a shared, appended-to list makes
 * visible. Booting a real Cassandra to assert it would test Cassandra; that belongs in
 * {@code integration-tests}, against the assembled tarball.
 *
 * <p>Implements {@link AutoCloseable} like {@code IsolatedService} does, so the tests also see
 * the loader-release call the supervisor makes after each stop.
 */
final class RecordingService implements EmbeddedService, AutoCloseable {

    private final String name;
    private final List<String> calls;
    private final AtomicBoolean stopped = new AtomicBoolean();

    private volatile ServiceStatus status = ServiceStatus.NEW;
    private volatile ServiceContext context;

    /** When set, {@code start()} throws it instead of coming up. */
    volatile Exception startFailure;
    /** When set, {@code prepareDecommission()} throws it — what a refused decommission looks like. */
    volatile Exception prepareFailure;
    /** When set, {@code decommission()} throws it, past the point where backing out is possible. */
    volatile Exception decommissionFailure;
    /** When set, {@code abortDecommission()} throws it: compensation itself can fail. */
    volatile Exception abortFailure;
    /** Reported as {@code hostId} in {@link #details()}; the supervisor names OpenSearch after it. */
    volatile String hostId;
    /** What {@code awaitDecommissionReady} concludes, standing in for "no shards left here". */
    volatile boolean handsOffCleanly = true;
    /** When false, {@code awaitDecommissionReady} burns its whole deadline before giving up. */
    volatile boolean waitsForItsDeadline;
    /** When set, {@code prepareDecommission} blocks on it until a test opens it. */
    volatile CountDownLatch prepareGate;
    /**
     * When set, {@code awaitDecommissionReady} blocks on it. Holds a decommission open in the
     * phase it spends nearly all its time in, so a test can observe what happens meanwhile.
     */
    volatile CountDownLatch relocationGate;
    /** The deadline the coordinator handed each phase, latest last. */
    final Map<String, Duration> phaseTimeouts = new LinkedHashMap<>();
    /** What {@code abortDecommission} saw on its context, for the cancelled-decommission case. */
    volatile boolean cancelledDuringAbort;

    RecordingService(String name, List<String> calls) {
        this.name = name;
        this.calls = calls;
    }

    private void record(String call) {
        synchronized (calls) {
            calls.add(name + '.' + call);
        }
    }

    /** Bounded, so a test that forgets to open a gate fails rather than hanging the build. */
    private static void awaitGate(CountDownLatch gate) throws InterruptedException {
        if (gate != null) {
            gate.await(1, TimeUnit.MINUTES);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void start(ServiceContext serviceContext) throws Exception {
        record("start");
        this.context = serviceContext;
        if (startFailure != null) {
            status = ServiceStatus.FAILED;
            throw startFailure;
        }
        status = ServiceStatus.RUNNING;
    }

    @Override
    public ServiceStatus status() {
        return status;
    }

    @Override
    public Map<String, String> details() {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("status", status.name());
        if (hostId != null) {
            details.put("hostId", hostId);
        }
        return details;
    }

    @Override
    public void prepareDecommission(DecommissionContext decommission) throws Exception {
        record("prepareDecommission");
        phaseTimeouts.put("prepareDecommission", decommission.timeout());
        awaitGate(prepareGate);
        status = ServiceStatus.DECOMMISSIONING;
        if (prepareFailure != null) {
            throw prepareFailure;
        }
    }

    @Override
    public void abortDecommission(DecommissionContext decommission) throws Exception {
        record("abortDecommission");
        phaseTimeouts.put("abortDecommission", decommission.timeout());
        cancelledDuringAbort = decommission.isCancelled();
        if (abortFailure != null) {
            throw abortFailure;
        }
        if (status == ServiceStatus.DECOMMISSIONING) {
            status = ServiceStatus.RUNNING;
        }
    }

    @Override
    public boolean awaitDecommissionReady(DecommissionContext decommission) throws Exception {
        record("awaitDecommissionReady");
        phaseTimeouts.put("awaitDecommissionReady", decommission.timeout());
        awaitGate(relocationGate);
        if (waitsForItsDeadline) {
            // What the real OpenSearch runtime does when shards will not move: poll until the
            // deadline in the context expires, then report that it gave up.
            Thread.sleep(decommission.timeout().toMillis());
        }
        return handsOffCleanly;
    }

    @Override
    public void decommission(DecommissionContext decommission) throws Exception {
        record("decommission");
        phaseTimeouts.put("decommission", decommission.timeout());
        if (decommissionFailure != null) {
            throw decommissionFailure;
        }
        if (!handsOffCleanly && !decommission.force()) {
            throw new ServiceException(name, "data is still on this node");
        }
        status = ServiceStatus.DECOMMISSIONED;
    }

    @Override
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        record("stop");
        if (status != ServiceStatus.DECOMMISSIONED) {
            status = ServiceStatus.STOPPED;
        }
    }

    @Override
    public void close() {
        record("close");
        stop();
    }

    /** The context handed to {@code start()}, for asserting on the settings the supervisor derived. */
    ServiceContext context() {
        return context;
    }

    void reportFatalError(String message) {
        context.reportFatalError(message, null);
    }

    /** Dies quietly, the way a service does when a background thread of its own gives up. */
    void fail() {
        status = ServiceStatus.FAILED;
    }
}
