/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import io.cassandraopensearch.spi.DecommissionContext;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/** Records what the service reports back so the tests can assert on progress, not just outcome. */
final class TestDecommissionContext implements DecommissionContext {

    record Progress(int percentComplete, String message) {
    }

    private final Duration timeout;
    private final boolean force;
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final List<Progress> progress = new CopyOnWriteArrayList<>();

    TestDecommissionContext(Duration timeout, boolean force) {
        this.timeout = timeout;
        this.force = force;
    }

    List<Progress> progress() {
        return progress;
    }

    void cancel() {
        cancelled.set(true);
    }

    @Override
    public Duration timeout() {
        return timeout;
    }

    @Override
    public boolean force() {
        return force;
    }

    @Override
    public void reportProgress(int percentComplete, String message) {
        progress.add(new Progress(percentComplete, message));
    }

    @Override
    public boolean isCancelled() {
        return cancelled.get();
    }
}
