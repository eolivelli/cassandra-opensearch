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
     * When true, the {@link #prepareGate} wait <b>ignores interrupts</b>, the way an OpenSearch
     * cluster-state update does. This is what makes an exclusion that overran its deadline
     * genuinely still inside the service when the supervisor gives up on it, rather than unwinding
     * on the interrupt {@code TimeLimited} sends and leaving nothing to be abandoned.
     */
    volatile boolean prepareIgnoresInterrupts;
    /**
     * When set, {@code awaitDecommissionReady} blocks on it. Holds a decommission open in the
     * phase it spends nearly all its time in, so a test can observe what happens meanwhile.
     */
    volatile CountDownLatch relocationGate;
    /**
     * When set, {@code start()} blocks on it and <b>ignores interrupts</b> — which is what
     * Cassandra's {@code daemon.activate()} does. This is what an abandoned startup looks like.
     */
    volatile CountDownLatch startGate;
    /** When set, {@code stop()} blocks on it; a shutdown wedged where a real drain would wedge. */
    volatile CountDownLatch stopGate;
    /**
     * When true, the {@link #stopGate} wait <b>ignores interrupts</b>, the way Cassandra's
     * {@code drain()} does. This is what makes a {@code stop()} that overran its deadline
     * genuinely still running when the supervisor gives up on it, rather than politely returning.
     */
    volatile boolean stopIgnoresInterrupts;
    /**
     * When set, {@code decommission()} blocks on it and never looks at {@code isCancelled()} —
     * which is exactly what {@code StorageService.decommission()} does. The phase a shutdown
     * cannot cancel.
     */
    volatile CountDownLatch decommissionGate;
    /**
     * When set, {@code status()} blocks on it, ignoring interrupts. A runtime whose status call
     * has wedged; the supervisor must not be holding a lock while it waits for one.
     */
    volatile CountDownLatch statusGate;
    /** Opened as {@code status()} enters {@link #statusGate}, so a test can wait for the wedge. */
    final CountDownLatch statusWedged = new CountDownLatch(1);
    /** When set, {@code stop()} throws it — including an {@code Error}, which a closed loader gives. */
    volatile Throwable stopFailure;
    /** When set, {@code status()} throws it once every service call is over. */
    volatile Throwable statusFailureAfterStop;
    /** When true, {@code status()} returns null after the service has been stopped. */
    volatile boolean nullStatusAfterStop;
    /** True once {@code stop()} has actually returned — not merely been entered. */
    volatile boolean stopReturned;
    /** Run on entry to {@code awaitDecommissionReady}, so a test can cancel inside the long wait. */
    volatile Runnable onAwaitDecommissionReady;
    /**
     * Run on entry to {@code prepareDecommission}. Lets a test cancel during a phase that — like
     * the real ones — never looks at {@code isCancelled()}, so only the coordinator can catch it.
     */
    volatile Runnable onPrepareDecommission;
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
        awaitGate(gate, null);
    }

    /**
     * Waits on a gate while polling the phase's cancellation flag.
     *
     * <p>The polling is the point. {@code awaitDecommissionReady} is the one phase the real
     * OpenSearch runtime spends minutes inside, and it is the phase that honours a cancellation by
     * checking {@code isCancelled()} between polls. A fake that only slept could not tell a
     * coordinator that cancels from one that does not, which is how a "cancelled decommission"
     * test came to assert nothing about cancellation.
     */
    private static void awaitGate(CountDownLatch gate, DecommissionContext context)
            throws InterruptedException {
        if (gate == null) {
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
        while (!gate.await(20, TimeUnit.MILLISECONDS)) {
            if (context != null && context.isCancelled()) {
                return;
            }
            if (System.nanoTime() - deadline > 0) {
                return;
            }
        }
    }

    /** Ignores interrupts, the way a native server startup does. */
    private static void awaitUninterruptibly(CountDownLatch gate) {
        if (gate == null) {
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
        while (System.nanoTime() - deadline < 0) {
            try {
                if (gate.await(20, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException swallowed) {
                // Deliberate: this is the behaviour that makes an abandoned start() dangerous.
            }
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
        awaitUninterruptibly(startGate);
        if (startFailure != null) {
            status = ServiceStatus.FAILED;
            throw startFailure;
        }
        status = ServiceStatus.RUNNING;
    }

    @Override
    public ServiceStatus status() {
        CountDownLatch wedge = statusGate;
        if (wedge != null) {
            statusWedged.countDown();
            awaitUninterruptibly(wedge);
        }
        if (stopped.get()) {
            Throwable failure = statusFailureAfterStop;
            if (failure != null) {
                sneakyThrow(failure);
            }
            if (nullStatusAfterStop) {
                // What a delegate returns when it can no longer work its state out. The supervisor
                // must survive it: it feeds a ConcurrentHashMap, which rejects nulls.
                return null;
            }
        }
        return status;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
        throw (T) t;
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
        Runnable onEntry = onPrepareDecommission;
        if (onEntry != null) {
            onEntry.run();
        }
        if (prepareIgnoresInterrupts) {
            awaitUninterruptibly(prepareGate);
        } else {
            awaitGate(prepareGate);
        }
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
        Runnable onEntry = onAwaitDecommissionReady;
        if (onEntry != null) {
            onEntry.run();
        }
        if (decommission.isCancelled()) {
            return false;
        }
        awaitGate(relocationGate, decommission);
        if (decommission.isCancelled()) {
            // The contract: gave up with work outstanding, without saying why. The coordinator is
            // the one that knows it was cancelled.
            return false;
        }
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
        // Deliberately without a cancellation check: this is the phase that has none, and the
        // point of the gate is to hold a shutdown against a decommission it cannot cancel.
        awaitUninterruptibly(decommissionGate);
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
        // The CAS is what both real runtimes do, and it is why an abandoned start() must never be
        // stopped: whoever wins it takes the one stop() the service will ever honour.
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        record("stop");
        if (stopIgnoresInterrupts) {
            awaitUninterruptibly(stopGate);
        } else {
            try {
                awaitGate(stopGate);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        Throwable failure = stopFailure;
        if (failure != null) {
            sneakyThrow(failure);
        }
        if (status != ServiceStatus.DECOMMISSIONED) {
            status = ServiceStatus.STOPPED;
        }
        stopReturned = true;
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
