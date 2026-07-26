/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs one lifecycle call on a borrowed thread so a deadline can be put on it.
 *
 * <p>Every SPI lifecycle method blocks: {@code start()} returns when the server is serving,
 * {@code decommission()} returns when the ranges have streamed. Bounding them therefore needs a
 * second thread — there is nothing to poll. The thread is created per call rather than pooled
 * because a call that overruns its deadline is by definition still running, and a pooled thread
 * would then be unavailable for the shutdown that follows.
 *
 * <p>An overrunning call is interrupted and abandoned, not killed — nothing can kill it. The
 * thread is a daemon so the corpse cannot by itself keep the JVM alive; a service that has
 * already spawned non-daemon threads of its own is a separate problem, which is why the
 * supervisor still walks the ordered shutdown afterwards.
 *
 * <h2>Interrupts belong to the caller, not to the call</h2>
 *
 * The deadline is the only thing that ends a wait here. An interrupt of the <i>waiting</i> thread
 * is remembered and put back, never obeyed, for the same reason {@code
 * Supervisor.awaitShutdownInFlight} does the same: the callers are shutdown paths, an interrupt
 * during shutdown is ordinary — every executor being torn down sends some — and there is nothing
 * useful to do with one. Returning on it would report a call as over while it is still inside the
 * service, and re-asserting the flag before the next call in the same walk would make {@code
 * FutureTask.get} fail at its entry check, collapsing an entire ordered shutdown in milliseconds.
 */
final class TimeLimited {

    private static final Logger LOG = LoggerFactory.getLogger(TimeLimited.class);

    private TimeLimited() {
    }

    @FunctionalInterface
    interface Action {
        void run() throws Exception;
    }

    /**
     * Thrown when the deadline expired and the call was left running.
     *
     * <p>Distinct from a plain {@link SupervisorException} because the two mean opposite things
     * to the caller. A call that <i>failed</i> is over, and whatever it touched can be cleaned
     * up. A call that was abandoned is still executing on a thread nothing can stop, still
     * inside the service's own code, and may yet mutate that service after the supervisor has
     * given up on it — so the supervisor must not then stop it, close its ClassLoader, or
     * pretend the process can exit cleanly.
     */
    static final class AbandonedException extends SupervisorException {

        private static final long serialVersionUID = 1L;

        AbandonedException(String message) {
            super(message);
        }
    }

    static void run(String description, Duration timeout, Action action) throws Exception {
        call(description, timeout, () -> {
            action.run();
            return null;
        });
    }

    /**
     * Runs {@code action} on a thread of its own and waits {@code timeout} for it.
     *
     * @return whatever the call returned
     * @throws AbandonedException if the deadline expired with the call still running. This is the
     *                            <b>only</b> way this method returns while the call is still inside
     *                            the service, and the distinction is the whole reason the class
     *                            exists: everything else that comes out of here is a call that is
     *                            over.
     */
    static <T> T call(String description, Duration timeout, Callable<T> action) throws Exception {
        FutureTask<T> task = new FutureTask<>(action);
        Thread worker = new Thread(task, "supervisor-" + description.replace(' ', '-'));
        worker.setDaemon(true);
        worker.start();
        long deadline = System.nanoTime() + timeout.toNanos();
        boolean interrupted = false;
        try {
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                try {
                    return task.get(remaining, TimeUnit.NANOSECONDS);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof Exception checked) {
                        throw checked;
                    }
                    throw new SupervisorException(description + " failed: " + cause, cause);
                } catch (TimeoutException e) {
                    break;
                } catch (InterruptedException e) {
                    // Remembered and put back below, but not obeyed; see the class comment. Note
                    // that an interrupt already pending when this method was entered lands here
                    // too, on the first get() — and clears the flag, so the wait that follows is
                    // the real one rather than another instant failure.
                    interrupted = true;
                    LOG.warn("Interrupted while waiting for {}; still waiting, because returning"
                            + " now would report a call as finished while it is still running"
                            + " inside the service", description);
                }
            }
            worker.interrupt();
            throw new AbandonedException(
                    description + " did not complete within " + format(timeout)
                            + "; the call has been interrupted and abandoned");
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Waits up to {@code budget} for a latch, treating an interrupt the way {@link #call} does:
     * remembered, put back on the way out, never obeyed.
     *
     * <p>The callers wait on latches that say "the thread that was driving these services has let
     * go of them", and what they do when the wait comes back false is abandon those services —
     * never stopped, never closed, exit code non-zero, the operator told to SIGKILL. Returning
     * false because the waiter was interrupted, rather than because the budget expired, therefore
     * turns an ordinary interrupt into a permanently broken node, and does it after 0 ms while the
     * message says the whole budget elapsed.
     *
     * @return true if the latch opened within the budget
     */
    static boolean await(CountDownLatch latch, Duration budget) {
        long deadline = System.nanoTime() + budget.toNanos();
        boolean interrupted = false;
        try {
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return latch.getCount() == 0;
                }
                try {
                    return latch.await(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    LOG.warn("Interrupted while waiting up to {} for an in-flight call to let go of"
                            + " the services; still waiting, because giving up here abandons them",
                            format(budget));
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Renders a duration the way the configuration file writes one, e.g. {@code 5m}. */
    static String format(Duration duration) {
        if (duration.isZero()) {
            return "0s";
        }
        if (duration.isNegative()) {
            return "-" + format(duration.negated());
        }
        // Sub-millisecond first: every test below divides toMillis(), which is 0 here, and 0 is
        // divisible by everything — so a 500µs deadline used to be reported as "0h".
        if (duration.toMillis() == 0) {
            return duration.toNanos() + "ns";
        }
        if (duration.toMillis() % 3_600_000 == 0) {
            return duration.toHours() + "h";
        }
        if (duration.toMillis() % 60_000 == 0) {
            return duration.toMinutes() + "m";
        }
        if (duration.toMillis() % 1000 == 0) {
            return duration.toSeconds() + "s";
        }
        return duration.toMillis() + "ms";
    }
}
