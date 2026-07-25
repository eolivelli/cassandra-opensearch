/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A decommission the supervisor was never told about.
 *
 * <p>{@code bin/nodetool decommission} goes straight to Cassandra's {@code StorageService} MBean.
 * Nothing in that path involves the supervisor, so OpenSearch is not asked to relocate anything
 * and, without a watcher, would still be holding this node's shards when the operator stops the
 * process. This test does exactly that — the real {@code nodetool}, on a real two-node cluster —
 * and asserts that the supervisor noticed and excluded the node from shard allocation anyway.
 *
 * <p>What it does <b>not</b> assert is safety, because the catch-up is not safe: by the time
 * Cassandra reports {@code LEAVING} the ring transition has already started, and on a node
 * holding real index data the relocation will not finish first. The property under test is that
 * the relocation begins at all. {@code TwoNodeDecommissionIT} is where the ordering guarantee
 * lives, and it is a different procedure.
 *
 * <p>The window is comfortable rather than tight, which is what makes this test stable:
 * {@code StorageService.decommission} announces {@code LEAVING} and then sleeps
 * {@code RING_DELAY_MILLIS} — 30 seconds by default — before it streams a single range, and the
 * watcher polls once a second.
 *
 * <p>The process is deliberately still alive at the end. Cassandra's own comment on the line that
 * sets {@code DECOMMISSIONED} is "let op be responsible for killing the process", so a node
 * retired this way keeps running with a dead ring member and a live OpenSearch node — which is
 * precisely why the shards have somewhere to go and time to get there.
 */
@ExtendWith(DumpLogsOnFailure.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExternalDecommissionIT {

    private static final String INDEX = "it-external-decommission";
    private static final int DOCUMENTS = 20;

    /** Four shards over two nodes: enough that the balancer cannot leave one node empty. */
    private static final int SHARDS = 4;

    private static DistributionNode first;
    private static DistributionNode leaving;

    /** The leaving node's OpenSearch node name, which is its Cassandra host id. */
    private static String leavingNodeName;

    @BeforeAll
    static void startBothNodes() {
        ClusterTopology topology = ClusterTopology.of(NodeEndpoints.FIRST, NodeEndpoints.SECOND);
        first = DistributionNode.install(
                Distribution.workDirectory(ExternalDecommissionIT.class, "first"),
                "first", NodeEndpoints.FIRST, topology);
        leaving = DistributionNode.install(
                Distribution.workDirectory(ExternalDecommissionIT.class, "leaving"),
                "leaving", NodeEndpoints.SECOND, topology);

        first.start();
        // Sequential: the second node bootstraps from the first, and OpenSearch's cluster is
        // bootstrapped by the first node's transport address.
        leaving.start();
    }

    @AfterAll
    static void tearDown() {
        try {
            for (DistributionNode node : List.of(leaving, first)) {
                if (node != null && node.isAlive()) {
                    node.stop();
                }
            }
        } finally {
            for (DistributionNode node : List.of(leaving, first)) {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    @Test
    @Order(1)
    void bothNodesJoinedAndTheLeavingOneHoldsShards() {
        Await.until("the ring has two nodes", Duration.ofMinutes(2),
                () -> first.nodetool("status").output(),
                () -> upNodes(first.nodetool("status").output()) == 2);
        Await.until("the OpenSearch cluster has two nodes", Duration.ofMinutes(2),
                () -> first.rest().cat("/_cat/nodes?h=name,ip"),
                () -> first.rest().cat("/_cat/nodes?h=name,ip").lines().count() == 2);

        leavingNodeName = SupervisorStatus.of(leaving.cli("status")).value("node.name");
        assertThat(leavingNodeName).isNotBlank();

        Rest rest = first.rest();
        assertThat(rest.put('/' + INDEX, """
                {"settings":{"number_of_shards":%d,"number_of_replicas":0}}""".formatted(SHARDS))
                .status()).isEqualTo(200);
        for (int i = 0; i < DOCUMENTS; i++) {
            assertThat(rest.put('/' + INDEX + "/_doc/" + i, """
                    {"title":"document %d"}""".formatted(i)).status()).isEqualTo(201);
        }

        // Without shards here there is nothing for the catch-up to relocate, and the assertions
        // below would pass for the wrong reason.
        Await.until("the leaving node has been given shards", Duration.ofMinutes(2),
                () -> first.rest().cat("/_cat/shards/" + INDEX + "?h=shard,state,node"),
                () -> !shardsOn(leavingNodeName).isEmpty());

        assertThat(exclusions())
                .as("nothing may be excluded before the decommission starts")
                .doesNotContain(leavingNodeName);
    }

    /**
     * {@code bin/nodetool decommission --force} — the supervisor is bypassed entirely.
     *
     * <p>{@code --force} for the same reason as in {@code TwoNodeDecommissionIT}: {@code
     * system_distributed} is RF 3 and Cassandra refuses an unforced decommission at N=2.
     */
    @Test
    @Order(2)
    void theSupervisorExcludesTheNodeItWasNeverToldWasLeaving() {
        Commands.Result decommission = leaving.run(Duration.ofMinutes(10),
                "bin/nodetool", "decommission", "--force");
        assertThat(decommission.succeeded())
                .as("nodetool decommission failed:%n%s", decommission.describe()).isTrue();

        Await.until("the leaving node is excluded from shard allocation", Duration.ofMinutes(2),
                ExternalDecommissionIT::exclusions,
                () -> exclusions().contains(leavingNodeName));

        String log = leaving.supervisorLog();
        assertThat(log)
                .as("the operator has to be told this was not the ordering-safe path:%n%s", log)
                .contains("was not asked through the supervisor")
                .contains("bin/cassandra-opensearch decommission");

        // The supervisor must not have mistaken this for its own decommission and started running
        // the coordinated procedure alongside Cassandra's.
        assertThat(log).doesNotContain("[await-shard-relocation]");
        assertThat(SupervisorStatus.of(leaving.cli("status")).value("State")).isEqualTo("RUNNING");
        assertThat(leaving.isAlive())
                .as("nodetool decommission leaves the process to the operator; nothing here exits it")
                .isTrue();
    }

    /**
     * The exclusion is what makes the relocation happen, and the relocation is the whole point.
     *
     * <p>It completes here only because this node's OpenSearch is still up and holds twenty tiny
     * documents. On a node with real index data it would still be moving them when the operator
     * stopped the process, and they would be gone — the index has no replicas. That is the race
     * documented in {@code docs/KNOWN-GAPS.md} §1, and no test can assert it away.
     */
    @Test
    @Order(3)
    void theShardsRelocateOffTheRetiredNode() {
        Await.until("every shard has left the retired node", Duration.ofMinutes(3),
                () -> first.rest().cat("/_cat/shards/" + INDEX + "?h=shard,state,node"),
                () -> shardsOn(leavingNodeName).isEmpty());

        assertThat(first.rest().cat("/_cat/shards/" + INDEX + "?h=state").lines().toList())
                .hasSize(SHARDS)
                .allMatch("STARTED"::equals);
        assertThat(first.rest().post('/' + INDEX + "/_search", """
                {"query":{"match_all":{}},"size":0}""").body())
                .contains("\"value\":" + DOCUMENTS);
    }

    // --- helpers ---------------------------------------------------------------------------

    /** {@code UN} rows in {@code nodetool status} — up and normal, which is what "joined" means. */
    private static long upNodes(String nodetoolStatus) {
        return nodetoolStatus.lines().filter(line -> line.startsWith("UN ")).count();
    }

    /**
     * {@code cluster.routing.allocation.exclude._name}, read from the surviving node.
     *
     * <p>Read as raw JSON. The value is a comma-separated list of node names under a settings
     * path, and asserting that the leaving node's name appears in the document at all is exactly
     * as strong as parsing it: no other setting in the response can contain that name.
     */
    private static String exclusions() {
        Rest.Response response = first.rest().get("/_cluster/settings?flat_settings=true");
        if (response.status() != 200) {
            throw new IllegalStateException("_cluster/settings returned " + response.status());
        }
        return response.body();
    }

    private static List<String> shardsOn(String nodeName) {
        List<String> shards = new ArrayList<>();
        for (String line : first.rest().cat("/_cat/shards/" + INDEX + "?h=shard,state,node").split("\n")) {
            if (line.contains(nodeName)) {
                shards.add(line.trim());
            }
        }
        return shards;
    }
}
