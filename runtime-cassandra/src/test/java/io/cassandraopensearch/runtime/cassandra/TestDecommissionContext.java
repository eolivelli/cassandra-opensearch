/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.cassandraopensearch.spi.DecommissionContext;

/** A {@link DecommissionContext} that records the progress the service reports. */
final class TestDecommissionContext implements DecommissionContext {

    private final Duration timeout;
    private final boolean force;
    private final List<String> progress = new CopyOnWriteArrayList<>();

    private volatile boolean cancelled;

    TestDecommissionContext(Duration timeout, boolean force) {
        this.timeout = timeout;
        this.force = force;
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
        progress.add(percentComplete + "% " + message);
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    List<String> progress() {
        return progress;
    }
}
