/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The coupled decommission, on a real two-node cluster.
 *
 * <p>This is the only test that can prove the product's one genuinely coupled operation, because
 * the ordering it exists to guarantee — OpenSearch relocates its shards <i>before</i> Cassandra
 * hands its ranges back — is unobservable with one node: Cassandra refuses a single-node
 * decommission outright, so nothing downstream of that refusal ever runs.
 *
 * <p>Two nodes, two processes, each a full installation of the tarball. The spike in
 * {@code docs/spikes/cassandra-embedding.md} ran its two nodes inside one JVM and had to work
 * around three constraints that do not apply here — a per-loader snapshot of {@code
 * cassandra.config}, one RMI hostname per JVM, one MBean server agent id — because two separate
 * processes have none of that shared state. What does still apply is:
 *
 * <ul>
 *   <li><b>Distinct listen addresses.</b> 127.0.0.1 and 127.0.0.2; Linux routes the whole
 *       127.0.0.0/8 to {@code lo} with no alias needed.</li>
 *   <li><b>Sequential startup.</b> The second node bootstraps from the first, and OpenSearch's
 *       cluster is bootstrapped by the first node's transport address.</li>
 *   <li><b>{@code --force} at N=2.</b> {@code system_distributed} has RF 3, so Cassandra refuses
 *       an unforced decommission with "Not enough live nodes to maintain replication factor".
 *       Three nodes would exercise the unforced path; two is what proves the ordering.</li>
 * </ul>
 *
 * <p>The user keyspace is RF 1 on purpose. At RF 2 the surviving node would already hold every
 * row and "the data survived" would prove nothing about streaming; at RF 1 the rows the leaving
 * node owns exist nowhere else until it hands them over.
 */
@ExtendWith(DumpLogsOnFailure.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TwoNodeDecommissionIT {

    private static final String KEYSPACE = "it_two_node";
    private static final String INDEX = "it-two-node";
    private static final int ROWS = 200;
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
                Distribution.workDirectory(TwoNodeDecommissionIT.class, "first"),
                "first", NodeEndpoints.FIRST, topology);
        leaving = DistributionNode.install(
                Distribution.workDirectory(TwoNodeDecommissionIT.class, "leaving"),
                "leaving", NodeEndpoints.SECOND, topology);

        first.start();
        // Sequential: the second node bootstraps from the first, and OpenSearch's cluster is
        // bootstrapped by the first node's transport address. Starting them together is a race
        // between a join and a second election.
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
    void bothNodesJoinedBothClusters() {
        Await.until("the ring has two nodes", Duration.ofMinutes(2),
                () -> first.nodetool("status").output(),
                () -> upNodes(first.nodetool("status").output()) == 2);

        Await.until("the OpenSearch cluster has two nodes", Duration.ofMinutes(2),
                () -> first.rest().cat("/_cat/nodes?h=name,ip"),
                () -> first.rest().cat("/_cat/nodes?h=name,ip").lines().count() == 2);

        leavingNodeName = SupervisorStatus.of(leaving.cli("status")).value("node.name");
        assertThat(leavingNodeName).isNotBlank();
        assertThat(SupervisorStatus.of(leaving.cli("status")).value("operationMode")).isEqualTo("NORMAL");
    }

    @Test
    @Order(2)
    void dataIsWrittenToBothClusters() {
        try (CqlSession session = session(first)) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
                    + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".probe (k int PRIMARY KEY, v text)");
            for (int i = 0; i < ROWS; i++) {
                session.execute("INSERT INTO " + KEYSPACE + ".probe (k, v) VALUES (" + i + ", 'row-" + i + "')");
            }
            assertThat(count(session)).isEqualTo(ROWS);
        }

        Rest rest = first.rest();
        assertThat(rest.put('/' + INDEX, """
                {"settings":{"number_of_shards":%d,"number_of_replicas":0}}""".formatted(SHARDS))
                .status()).isEqualTo(200);
        for (int i = 0; i < DOCUMENTS; i++) {
            assertThat(rest.put('/' + INDEX + "/_doc/" + i, """
                    {"title":"document %d","n":%d}""".formatted(i, i)).status()).isEqualTo(201);
        }
        assertThat(rest.post('/' + INDEX + "/_refresh", "").status()).isEqualTo(200);

        // Without shards on the leaving node there is no relocation to order, and the decommission
        // below would pass for the wrong reason.
        assertThat(shardsOn(leavingNodeName))
                .as("the leaving node must hold shards before it is decommissioned")
                .isNotEmpty();
    }

    /**
     * {@code bin/cassandra-opensearch decommission --force} on the leaving node.
     *
     * <p>The ordering is asserted from the leaving node's own log, which is a sequential record
     * written by a single coordinator: the line saying every shard has left this node must appear
     * before the line that starts Cassandra's ranges streaming, and that before the line saying it
     * left the ring. Reversing those two steps is the failure this whole procedure exists to
     * prevent, and it would look like a successful decommission that silently dropped indices.
     */
    @Test
    @Order(3)
    void openSearchRelocatesItsShardsBeforeCassandraLeavesTheRing() {
        Commands.Result decommission = leaving.run(Duration.ofMinutes(30),
                "bin/cassandra-opensearch", "decommission", "--force");

        assertThat(decommission.succeeded())
                .as("decommission failed:%n%s", decommission.describe()).isTrue();
        assertThat(decommission.output())
                .contains("OpenSearch relocated its shards")
                .contains("Cassandra streamed its ranges and left the ring");

        String log = leaving.supervisorLog();
        int relocated = log.indexOf("[await-shard-relocation] relocated all");
        int streaming = log.indexOf("[decommission-cassandra] streaming ranges");
        int left = log.indexOf("[decommission-cassandra] left the ring");

        assertThat(relocated).as("no completed shard relocation in the log:%n%s", log).isNotNegative();
        assertThat(streaming).as("Cassandra never started streaming").isGreaterThan(relocated);
        assertThat(left).as("Cassandra never left the ring").isGreaterThan(streaming);

        // Step 7 of the procedure is `exit 0`; the process retires itself, with no stop command.
        leaving.awaitExit(Duration.ofMinutes(2));
        assertThat(leaving.wasKilled()).isFalse();
        assertThat(leaving.supervisorLog()).contains("Shutdown complete; exit code 0");
    }

    @Test
    @Order(4)
    void theRingConvergedAndTheDataSurvived() {
        Await.until("the ring is down to one node", Duration.ofMinutes(3),
                () -> first.nodetool("status").output(),
                () -> upNodes(first.nodetool("status").output()) == 1);
        Await.until("the OpenSearch cluster is down to one node", Duration.ofMinutes(3),
                () -> first.rest().cat("/_cat/nodes?h=name"),
                () -> first.rest().cat("/_cat/nodes?h=name").lines().count() == 1);

        // Every shard is now on the surviving node, and every document is still searchable. With
        // no replicas configured, a shard that had failed to relocate would simply be gone.
        assertThat(shardsOn(leavingNodeName)).isEmpty();
        assertThat(first.rest().cat("/_cat/shards/" + INDEX + "?h=state").lines().toList())
                .hasSize(SHARDS)
                .allMatch("STARTED"::equals);
        assertThat(first.rest().post('/' + INDEX + "/_search", """
                {"query":{"match_all":{}},"size":0}""").body())
                .contains("\"value\":" + DOCUMENTS);

        try (CqlSession session = session(first)) {
            assertThat(count(session))
                    .as("every row the leaving node owned had to stream to the survivor: the"
                            + " keyspace is RF 1, so it existed nowhere else")
                    .isEqualTo(ROWS);
            Row row = session.execute(
                    "SELECT v FROM " + KEYSPACE + ".probe WHERE k = " + (ROWS - 1)).one();
            assertThat(row).isNotNull();
            assertThat(row.getString("v")).isEqualTo("row-" + (ROWS - 1));
        }
    }

    // --- helpers ---------------------------------------------------------------------------

    private static CqlSession session(DistributionNode node) {
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(node.endpoints().host(), node.endpoints().nativePort()))
                .withLocalDatacenter("datacenter1")
                .build();
    }

    private static long count(CqlSession session) {
        return session.execute("SELECT count(*) FROM " + KEYSPACE + ".probe").one().getLong(0);
    }

    /** {@code UN} rows in {@code nodetool status} — up and normal, which is what "joined" means. */
    private static long upNodes(String nodetoolStatus) {
        return nodetoolStatus.lines().filter(line -> line.startsWith("UN ")).count();
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
