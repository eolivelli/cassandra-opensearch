/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server.config;

import java.time.Duration;
import java.util.List;

/** Timeouts and policy for the coupled decommission procedure. */
public final class DecommissionConfiguration {

    private final Duration shardRelocationTimeout;
    private final Duration ringStreamingTimeout;
    private final boolean watchExternalDecommission;

    private DecommissionConfiguration(
            Duration shardRelocationTimeout,
            Duration ringStreamingTimeout,
            boolean watchExternalDecommission) {
        this.shardRelocationTimeout = shardRelocationTimeout;
        this.ringStreamingTimeout = ringStreamingTimeout;
        this.watchExternalDecommission = watchExternalDecommission;
    }

    static DecommissionConfiguration fromYaml(YamlReader yaml) {
        DecommissionConfiguration configuration = new DecommissionConfiguration(
                yaml.duration("shard_relocation_timeout", Duration.ofMinutes(30)),
                yaml.duration("ring_streaming_timeout", Duration.ofHours(1)),
                yaml.bool("watch_external_decommission", true));
        yaml.rejectUnknownKeys(
                List.of("shard_relocation_timeout", "ring_streaming_timeout", "watch_external_decommission"));
        return configuration;
    }

    /** Default configuration, for tests and for an installation that omits the section. */
    public static DecommissionConfiguration defaults() {
        return new DecommissionConfiguration(Duration.ofMinutes(30), Duration.ofHours(1), true);
    }

    /**
     * How long to wait for OpenSearch to relocate every shard off this node before Cassandra is
     * allowed to leave the ring. Generous by default: this is bounded by how much index data the
     * node holds and how fast the cluster can move it.
     */
    public Duration shardRelocationTimeout() {
        return shardRelocationTimeout;
    }

    /** How long to wait for Cassandra to stream its ranges away. */
    public Duration ringStreamingTimeout() {
        return ringStreamingTimeout;
    }

    /**
     * Whether to watch for a decommission started outside the supervisor — a plain {@code
     * nodetool decommission} against Cassandra's JMX — and exclude the OpenSearch node as a
     * best-effort catch-up.
     *
     * <p>That path cannot be made fully safe, because by the time Cassandra reports {@code
     * LEAVING} the ring transition is already under way and OpenSearch may not finish relocating
     * before the process exits. The supported path remains {@code bin/cassandra-opensearch
     * decommission}, which orders the two properly.
     */
    public boolean watchExternalDecommission() {
        return watchExternalDecommission;
    }
}
