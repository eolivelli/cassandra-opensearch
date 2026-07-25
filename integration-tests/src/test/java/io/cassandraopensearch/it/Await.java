/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Polling with a deadline, in place of sleeping.
 *
 * <p>A fixed sleep is either slower than it needs to be or shorter than the machine's worst case,
 * and on a loaded CI agent it is reliably both. Everything these tests wait for — a ring
 * converging, shards relocating, a process exiting — is observable, so it is polled.
 *
 * <p>The failure message carries the last observation rather than just "timed out", because the
 * value the condition was still seeing at the deadline is usually the whole diagnosis.
 */
final class Await {

    private static final Duration INTERVAL = Duration.ofMillis(500);

    private Await() {
    }

    /** @param lastSeen consulted only when the deadline is reached, for the failure message */
    static void until(String what, Duration timeout, Supplier<String> lastSeen, Condition condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;
        while (true) {
            try {
                if (condition.isMet()) {
                    return;
                }
                lastFailure = null;
            } catch (Exception e) {
                // A node that is still starting refuses connections; that is a "not yet", not a
                // failure, right up until the deadline, where it becomes the reported cause.
                lastFailure = e;
            }
            if (System.nanoTime() - deadline > 0) {
                String observation;
                try {
                    observation = lastSeen.get();
                } catch (Exception e) {
                    observation = "could not be read: " + e;
                }
                throw new AssertionError("timed out after " + timeout + " waiting until " + what
                        + "\n  last seen: " + observation, lastFailure);
            }
            sleep(INTERVAL);
        }
    }

    static void until(String what, Duration timeout, Condition condition) {
        until(what, timeout, () -> "(nothing recorded)", condition);
    }

    static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting", e);
        }
    }

    @FunctionalInterface
    interface Condition {
        boolean isMet() throws Exception;
    }
}
