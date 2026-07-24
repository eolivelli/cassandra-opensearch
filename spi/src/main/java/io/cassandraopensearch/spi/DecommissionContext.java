/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.spi;

import java.time.Duration;

/**
 * Parameters and progress channel for a decommission in flight.
 *
 * <p>Decommission is the one operation where the two services are genuinely coupled: an
 * OpenSearch node holding primary shards must relocate them <i>before</i> Cassandra tears the
 * node out of the ring, because once Cassandra has left, the process is going away and any
 * shard still resident here is lost. The supervisor therefore drives:
 *
 * <ol>
 *   <li>{@link EmbeddedService#prepareDecommission} on both services</li>
 *   <li>{@link EmbeddedService#awaitDecommissionReady} on OpenSearch (shard relocation)</li>
 *   <li>{@link EmbeddedService#decommission} on Cassandra (ring streaming)</li>
 *   <li>{@link EmbeddedService#stop} on both, OpenSearch first</li>
 * </ol>
 */
public interface DecommissionContext {

    /** How long the caller is willing to wait for this phase before giving up. */
    Duration timeout();

    /**
     * If true, proceed even when data cannot be fully handed off — the operator has accepted
     * the risk. Maps to {@code --force} on the CLI.
     */
    boolean force();

    /**
     * Reports incremental progress so the CLI can render something useful during a long
     * relocation. Safe to call from any thread.
     *
     * @param percentComplete 0..100, or -1 when the total is not yet known
     * @param message         what is currently happening, e.g. {@code "relocating 3 of 12 shards"}
     */
    void reportProgress(int percentComplete, String message);

    /**
     * True once the operator has asked to abort. Long-running implementations must poll this
     * and unwind promptly when it flips.
     */
    boolean isCancelled();
}
