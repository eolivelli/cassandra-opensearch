/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.NoClusterManagerBlockService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.common.transport.TransportAddress;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the memoisation key does and does not distinguish.
 *
 * <p>The interesting state is the one a node applies the moment it loses its cluster manager, and
 * the interesting thing about it is that {@code Coordinator.clusterStateWithNoClusterManagerBlock}
 * builds it <i>without</i> {@code incrementVersion()}. {@code ClusterState.Builder(ClusterState)}
 * copies the version and the uuid, and {@code build()} replaces the uuid only when it is still
 * {@code _na_} — so the derived state carries both of its parent's identifiers. A cache keyed on
 * either, or on both, serves the pre-failure snapshot for exactly as long as the node has no
 * cluster manager: the window in which somebody is reading {@code details()} to find out what
 * happened.
 */
class ClusterSnapshotTest {

    private static final String LOCAL_ID = "local-node";

    @Test
    void neitherTheUuidNorTheVersionSurvivesTheLossOfTheClusterManager() {
        ClusterState settled = settledState();
        ClusterState lost = withoutClusterManager(settled);

        // Not an incidental detail of this fixture: it is the reason the key needs a third
        // component, and if OpenSearch ever starts incrementing here this test is what will say so.
        assertThat(lost.stateUUID()).isEqualTo(settled.stateUUID());
        assertThat(lost.version()).isEqualTo(settled.version());
    }

    @Test
    void theKeyTellsThoseTwoStatesApart() {
        ClusterState settled = settledState();
        ClusterState lost = withoutClusterManager(settled);

        assertThat(ClusterSnapshot.key(lost))
                .as("a stale snapshot here is served precisely while the cluster is in trouble")
                .isNotEqualTo(ClusterSnapshot.key(settled));
    }

    @Test
    void thatSameStateAlwaysHasTheSameKey() {
        ClusterState settled = settledState();

        assertThat(ClusterSnapshot.key(settled)).isEqualTo(ClusterSnapshot.key(settled));
        assertThat(ClusterSnapshot.of(settled, LOCAL_ID).key()).isEqualTo(ClusterSnapshot.key(settled));
    }

    /** A cluster with one node, which is also its cluster manager. */
    private static ClusterState settledState() {
        DiscoveryNode local = new DiscoveryNode(
                "local",
                LOCAL_ID,
                new TransportAddress(TransportAddress.META_ADDRESS, 0),
                java.util.Map.of(),
                Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT);
        return ClusterState.builder(new ClusterName("snapshot-test"))
                .nodes(DiscoveryNodes.builder().add(local).localNodeId(LOCAL_ID).clusterManagerNodeId(LOCAL_ID).build())
                .build();
    }

    /**
     * What {@code Coordinator.clusterStateWithNoClusterManagerBlock} does, reproduced: add the
     * no-cluster-manager global block, clear the cluster manager node id, and build — with no
     * {@code incrementVersion()} anywhere.
     */
    private static ClusterState withoutClusterManager(ClusterState state) {
        return ClusterState.builder(state)
                .blocks(ClusterBlocks.builder()
                        .blocks(state.blocks())
                        .addGlobalBlock(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_WRITES)
                        .build())
                .nodes(DiscoveryNodes.builder(state.nodes()).clusterManagerNodeId(null).build())
                .build();
    }
}
