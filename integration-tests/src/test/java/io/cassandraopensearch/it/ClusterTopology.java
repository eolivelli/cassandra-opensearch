/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import java.util.Arrays;
import java.util.List;

/**
 * What a node has to be told about its neighbours before it boots.
 *
 * <p>Both clusters need this and neither can be told it afterwards: Cassandra reads
 * {@code seed_provider} in {@code DatabaseDescriptor} at daemon initialisation, and OpenSearch
 * resolves {@code discovery.seed_hosts} and {@code cluster.initial_cluster_manager_nodes} while
 * {@code Node} is being constructed.
 *
 * <p>{@code openSearchSeedHosts} being empty means "leave {@code discovery.type: single-node} as
 * the distribution ships it". That is the shipped configuration and the one the single-node ITs
 * must exercise; a multi-node cluster has to replace it, because a single-node-discovery node
 * elects itself and never forms a cluster with anyone.
 *
 * @param cassandraSeed        {@code host:port} of the seed node, itself for the first node
 * @param openSearchSeedHosts  transport addresses of every OpenSearch node, or empty for
 *                             single-node discovery
 * @param openSearchBootstrap  {@code cluster.initial_cluster_manager_nodes}. Transport addresses
 *                             rather than node names, because a node's name is the Cassandra host
 *                             id it is given at runtime — see {@code Supervisor.derivedSettings}
 *                             — and nothing knows it before the process has booted.
 */
record ClusterTopology(String cassandraSeed, List<String> openSearchSeedHosts, List<String> openSearchBootstrap) {

    /** One node, alone in both clusters, configured exactly as the distribution ships. */
    static ClusterTopology alone(NodeEndpoints node) {
        return new ClusterTopology(node.storageAddress(), List.of(), List.of());
    }

    /**
     * A cluster whose first member bootstraps both rings.
     *
     * <p>Every node gets the same {@code cluster.initial_cluster_manager_nodes} — one entry, the
     * first node — which is what keeps the second node from bootstrapping a second cluster of its
     * own if it cannot reach the first one within its discovery timeout.
     */
    static ClusterTopology of(NodeEndpoints... nodes) {
        return new ClusterTopology(
                nodes[0].storageAddress(),
                Arrays.stream(nodes).map(NodeEndpoints::transportAddress).toList(),
                List.of(nodes[0].transportAddress()));
    }
}
