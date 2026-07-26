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
 * The pieces of a {@link ClusterState} that {@code details()} reports, derived once per applied
 * cluster state.
 *
 * <p>{@code EmbeddedService.details()} must not block, and the obvious implementation — a
 * {@code _cluster/health} call — does exactly that: it is a cluster-manager-node read that can
 * queue behind cluster state updates. Deriving from the locally applied state instead is a
 * volatile read plus arithmetic. The derivation itself is O(shards), so it is memoised against
 * {@link #key()}: repeated calls between two cluster state updates cost nothing.
 *
 * <h2>What the key is, and what it is not</h2>
 *
 * <p>{@link #key()} is the state UUID, the version and the cluster manager's node id together.
 * None of the three is sufficient alone, and the combination is still only a <i>staleness</i>
 * check, not an identity:
 *
 * <ul>
 *   <li><b>The version</b> is monotonic only within one cluster-manager term. Two different states
 *       from two terms can carry the same version.</li>
 *   <li><b>The state UUID</b> is <i>not</i> regenerated on every build, whatever its name suggests.
 *       {@code ClusterState.Builder(ClusterState)} copies the uuid from the state it is derived
 *       from, and {@code build()} replaces it only when it is still {@code _na_} — which only
 *       {@code incrementVersion()} arranges. So any state built from another without incrementing
 *       the version inherits both the uuid and the version of its parent. The state a node applies
 *       when it loses the cluster manager is exactly that case: {@code Coordinator}'s
 *       {@code clusterStateWithNoClusterManagerBlock} adds a global block and clears the cluster
 *       manager node id, and never calls {@code incrementVersion()}, so it is indistinguishable
 *       from its parent by uuid, by version, or by both.</li>
 *   <li><b>The cluster manager node id</b> is what tells those two apart, and it is in the key for
 *       that one reason: losing the cluster manager is precisely the moment an operator reads
 *       {@code details()} to find out what happened, and serving them the pre-failure snapshot
 *       until some later state arrives would be worst at the worst time.</li>
 * </ul>
 *
 * <p>Two states that differ in nothing but their blocks — a read-only index block, say — still
 * share a key and still serve the older snapshot. That is safe today, but not because the fields
 * below ignore {@code blocks()}: {@link #healthStatus} is derived from them. {@code
 * ClusterStateHealth} reports RED for any state whose {@code blocks()} carries a global block with
 * {@code RestStatus.SERVICE_UNAVAILABLE}, and in a running cluster the no-cluster-manager block is
 * the only such block — which is exactly the one the third component of the key covers, because it
 * is the only one added without a version bump. Every other block arrives on a state the cluster
 * manager published with an incremented version, so the key has already changed by the time it is
 * applied. Anything added to this record that reads {@code blocks()} outside that guarantee needs a
 * fourth key component, not a comment.
 */
record ClusterSnapshot(
        String key,
        String clusterName,
        String healthStatus,
        int shards,
        int primaries,
        int relocating,
        int initializing) {

    static ClusterSnapshot of(ClusterState state, String localNodeId) {
        RoutingNode local = state.getRoutingNodes().node(localNodeId);
        return new ClusterSnapshot(
                key(state),
                state.getClusterName().value(),
                new ClusterStateHealth(state).getStatus().name(),
                local == null ? 0 : local.size(),
                local == null ? 0 : local.numberOfOwningPrimaryShards(),
                local == null ? 0 : local.numberOfShardsWithState(ShardRoutingState.RELOCATING),
                local == null ? 0 : local.numberOfShardsWithState(ShardRoutingState.INITIALIZING));
    }

    /** The memoisation key of a state, computed without deriving anything O(shards). */
    static String key(ClusterState state) {
        return state.stateUUID() + '/' + state.version() + '/' + state.nodes().getClusterManagerNodeId();
    }
}
