/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.bootstrap;

import io.cassandraopensearch.spi.DecommissionContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** A {@link DecommissionContext} that never cancels and records the progress reported to it. */
final class ImmediateDecommissionContext implements DecommissionContext {

    private final List<String> progress = new ArrayList<>();

    List<String> progress() {
        return progress;
    }

    @Override
    public Duration timeout() {
        return Duration.ofSeconds(30);
    }

    @Override
    public boolean force() {
        return false;
    }

    @Override
    public void reportProgress(int percentComplete, String message) {
        progress.add(percentComplete + "% " + message);
    }

    @Override
    public boolean isCancelled() {
        return false;
    }
}
