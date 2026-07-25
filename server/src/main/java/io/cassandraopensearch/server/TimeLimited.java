/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import java.time.Duration;
import java.util.concurrent.Callable;
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
 */
final class TimeLimited {

    private TimeLimited() {
    }

    @FunctionalInterface
    interface Action {
        void run() throws Exception;
    }

    static void run(String description, Duration timeout, Action action) throws Exception {
        call(description, timeout, () -> {
            action.run();
            return null;
        });
    }

    static <T> T call(String description, Duration timeout, Callable<T> action) throws Exception {
        FutureTask<T> task = new FutureTask<>(action);
        Thread worker = new Thread(task, "supervisor-" + description.replace(' ', '-'));
        worker.setDaemon(true);
        worker.start();
        try {
            return task.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception checked) {
                throw checked;
            }
            throw new SupervisorException(description + " failed: " + cause, cause);
        } catch (TimeoutException e) {
            worker.interrupt();
            throw new SupervisorException(
                    description + " did not complete within " + format(timeout)
                            + "; the call has been interrupted and abandoned");
        } catch (InterruptedException e) {
            worker.interrupt();
            Thread.currentThread().interrupt();
            throw new SupervisorException(description + " was interrupted", e);
        }
    }

    /** Renders a duration the way the configuration file writes one, e.g. {@code 5m}. */
    static String format(Duration duration) {
        if (duration.isZero()) {
            return "0s";
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
