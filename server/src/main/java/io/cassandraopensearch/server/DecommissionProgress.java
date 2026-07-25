/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

/**
 * The last thing a decommission reported about itself.
 *
 * <p>A decommission can legitimately take an hour, and the operator's CLI is a separate process
 * connected over JMX. It cannot be handed a callback, so the supervisor keeps the most recent
 * progress here and the CLI polls it while its own {@code decommission} call blocks.
 *
 * @param phase           the step in progress, e.g. {@code "await-shard-relocation"}
 * @param percentComplete 0..100, or -1 when the total is not yet known
 */
public record DecommissionProgress(String phase, int percentComplete, String message) {

    static final DecommissionProgress NONE =
            new DecommissionProgress("idle", -1, "no decommission has been requested");

    @Override
    public String toString() {
        return percentComplete < 0
                ? phase + ": " + message
                : phase + ": " + percentComplete + "% - " + message;
    }
}
