/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRoutingState;

/**
 * The pieces of a {@link ClusterState} that {@code details()} reports, derived once per cluster
 * state version.
 *
 * <p>{@code EmbeddedService.details()} must not block, and the obvious implementation — a
 * {@code _cluster/health} call — does exactly that: it is a cluster-manager-node read that can
 * queue behind cluster state updates. Deriving from the locally applied state instead is a
 * volatile read plus arithmetic. The derivation itself is O(shards), so it is memoised against
 * {@link #version()}: repeated calls between two cluster state updates cost nothing.
 */
record ClusterSnapshot(
        long version,
        String clusterName,
        String healthStatus,
        int shards,
        int primaries,
        int relocating,
        int initializing) {

    static ClusterSnapshot of(ClusterState state, String localNodeId) {
        RoutingNode local = state.getRoutingNodes().node(localNodeId);
        return new ClusterSnapshot(
                state.version(),
                state.getClusterName().value(),
                new ClusterStateHealth(state).getStatus().name(),
                local == null ? 0 : local.size(),
                local == null ? 0 : local.numberOfOwningPrimaryShards(),
                local == null ? 0 : local.numberOfShardsWithState(ShardRoutingState.RELOCATING),
                local == null ? 0 : local.numberOfShardsWithState(ShardRoutingState.INITIALIZING));
    }
}
