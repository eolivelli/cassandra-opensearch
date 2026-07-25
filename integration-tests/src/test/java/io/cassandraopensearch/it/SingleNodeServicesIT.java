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
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Everything one running node must be able to do, asserted against the shipped process from
 * outside it: CQL over the native protocol, OpenSearch over HTTP, {@code nodetool} over JMX, and
 * the operating system's own view of which process owns which socket.
 *
 * <p>One node for the class. A cold start is tens of seconds and none of these tests damages the
 * node for the next one. The class is ordered anyway, so that
 * {@link #aSingleNodeDecommissionIsRefusedAndTheNodeKeepsServing()} runs last: it is the only
 * test here that asks the node to retire, and what it asserts — that a refusal leaves nothing
 * behind — is a claim about the node the tests before it were using.
 */
@ExtendWith(DumpLogsOnFailure.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SingleNodeServicesIT {

    private static final String KEYSPACE = "it_single_node";
    private static final String INDEX = "it-single-node";

    private static DistributionNode node;
    private static CqlSession session;

    @BeforeAll
    static void startNode() {
        node = DistributionNode.install(
                        Distribution.workDirectory(SingleNodeServicesIT.class, "node"),
                        "single", NodeEndpoints.FIRST, ClusterTopology.alone(NodeEndpoints.FIRST))
                .start();
        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(node.endpoints().host(), node.endpoints().nativePort()))
                .withLocalDatacenter("datacenter1")
                .build();
    }

    @AfterAll
    static void tearDown() {
        try {
            if (session != null) {
                session.close();
            }
            if (node != null && node.isAlive()) {
                Commands.Result stop = node.stop();
                assertThat(stop.succeeded()).as("stop failed:%n%s", stop.describe()).isTrue();
            }
        } finally {
            if (node != null) {
                node.close();
            }
        }
    }

    /**
     * The DataStax driver against the shipped node, from a JVM that has never heard of Cassandra's
     * classpath — which is the point. The driver's Netty and the node's Netty are different
     * versions in different processes here, as they are for any real client.
     */
    @Test
    @Order(1)
    void cqlRoundTrip() {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
                + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + ".probe (k text PRIMARY KEY, v text, n int)");
        session.execute("INSERT INTO " + KEYSPACE
                + ".probe (k, v, n) VALUES ('k', 'written over the native protocol', 42)");

        Row row = session.execute("SELECT k, v, n FROM " + KEYSPACE + ".probe WHERE k = 'k'").one();

        assertThat(row).isNotNull();
        assertThat(row.getString("v")).isEqualTo("written over the native protocol");
        assertThat(row.getInt("n")).isEqualTo(42);
    }

    @Test
    @Order(2)
    void openSearchRoundTrip() {
        Rest rest = node.rest();

        assertThat(rest.put('/' + INDEX, """
                {"settings":{"number_of_shards":1,"number_of_replicas":0}}""").status()).isEqualTo(200);
        assertThat(rest.put('/' + INDEX + "/_doc/1", """
                {"title":"indexed over real HTTP","n":42}""").status()).isEqualTo(201);
        assertThat(rest.post('/' + INDEX + "/_refresh", "").status()).isEqualTo(200);

        Rest.Response search = rest.post('/' + INDEX + "/_search", """
                {"query":{"match":{"title":"real"}}}""");

        assertThat(search.status()).isEqualTo(200);
        assertThat(search.body())
                .contains("\"_id\":\"1\"")
                .contains("indexed over real HTTP");
    }

    /**
     * The canary for the most expensive failure in this design.
     *
     * <p>OpenSearch resolves {@code XContentBuilderExtension} through the one-argument
     * {@code ServiceLoader.load(Class)}, which reads the <b>thread context</b> ClassLoader. Inside
     * a child loader with the TCCL still pointing at the application loader it finds nothing, and
     * {@code XContentBuilder.WRITERS} — a {@code static final} — is permanently short of the
     * writers for {@code TimeValue} and friends. The node then starts, reports healthy, indexes
     * and searches perfectly, and answers <b>400</b> to these three endpoints:
     * {@code {"error":{"reason":"no raw transformer found for class
     * org.opensearch.common.unit.TimeValue"}}}.
     *
     * <p>So an ordinary smoke test cannot see it, and neither can a health check. This can, and it
     * is the assertion that would fail first if anyone loosened the TCCL pinning in
     * {@code IsolatedService}.
     */
    @Test
    @Order(3)
    void theRestApiIsNotCorruptedByTheContextClassLoader() {
        Rest rest = node.rest();

        for (String path : List.of("/", "/_cluster/health?timeout=30s", "/_nodes/stats", "/_cluster/stats")) {
            Rest.Response response = rest.get(path);
            assertThat(response.status())
                    .as("GET %s returned %s: %s — see docs/spikes/opensearch-embedding.md,"
                            + " 'the one that matters: pin the TCCL'", path, response.status(), response.body())
                    .isEqualTo(200);
            assertThat(response.body()).doesNotContain("no raw transformer found");
        }

        // The response body has to carry the types the broken path cannot serialise, not merely
        // return 200: TimeValue and friends are what the missing writers were for.
        assertThat(rest.get("/_cluster/health?timeout=30s").body()).contains("\"status\":");
        assertThat(rest.get("/_nodes/stats").body()).contains("\"timestamp\":");
    }

    /**
     * Stock {@code nodetool} against the embedded node. Two separate things have to be right for
     * this to work at all: the federated MBean server (a bare private server has no platform
     * {@code MemoryMXBean}, and {@code NodeProbe.connect} fetches one before anything else), and
     * the {@code CASSANDRA_INCLUDE} shim that defeats the fork's string-comparison JDK check,
     * which rejects every JDK from 18 to 21.
     */
    @Test
    @Order(4)
    void nodetoolDrivesTheEmbeddedNode() {
        Commands.Result status = node.nodetool("status");
        assertThat(status.succeeded()).as("nodetool status failed:%n%s", status.describe()).isTrue();
        assertThat(status.output())
                .containsPattern("(?m)^UN\\s+" + node.endpoints().host().replace(".", "\\."))
                .contains("rack1");

        Commands.Result info = node.nodetool("info");
        assertThat(info.succeeded()).as("nodetool info failed:%n%s", info.describe()).isTrue();
        assertThat(info.output())
                .containsPattern("Gossip active\\s+:\\s+true")
                .containsPattern("Native Transport active:\\s+true")
                .contains("ID");

        Commands.Result describe = node.nodetool("describecluster");
        assertThat(describe.succeeded()).as("nodetool describecluster failed:%n%s", describe.describe()).isTrue();
        assertThat(describe.output()).contains("Cluster Information", "cassandra-opensearch");

        Commands.Result version = node.nodetool("version");
        assertThat(version.succeeded()).as("nodetool version failed:%n%s", version.describe()).isTrue();
        assertThat(version.output()).contains("ReleaseVersion");
    }

    /**
     * The product's whole claim, asserted from outside the process: one JVM is serving both CQL
     * and the OpenSearch REST API.
     *
     * <p>Nothing inside the process can establish this. Every in-process assertion is equally true
     * of two cooperating JVMs; only the kernel's file-descriptor table can say that the socket
     * listening on the CQL port and the socket listening on the HTTP port belong to the same
     * process.
     */
    @Test
    @Order(5)
    void bothServersRunInOneJvm() {
        assumeTrue(ListeningSockets.available(), "/proc is not readable; this check is Linux-only");

        Set<Integer> ports = ListeningSockets.of(node.pid());
        NodeEndpoints endpoints = node.endpoints();

        assertThat(ports)
                .as("pid %s must own both servers' listening sockets", node.pid())
                .contains(endpoints.nativePort(), endpoints.storagePort(),
                        endpoints.httpPort(), endpoints.transportPort(),
                        endpoints.cassandraJmxPort(), endpoints.supervisorJmxPort());

        List<ProcessHandle> jvms = ProcessHandle.allProcesses()
                .filter(process -> process.info().commandLine()
                        .filter(line -> line.contains("cassandra-opensearch.home=" + node.home()))
                        .isPresent())
                .toList();
        assertThat(jvms).as("exactly one JVM may be serving this installation").hasSize(1);
        assertThat(jvms.get(0).pid()).isEqualTo(node.pid());
    }

    /**
     * A node alone in the ring cannot be decommissioned, and must survive being asked.
     *
     * <p>Cassandra refuses before it looks at {@code --force} — there is nowhere for the data to
     * go — so the supervisor refuses first, with an explanation. What matters as much as the
     * refusal is what it leaves behind: everything before step 3 of the procedure is reversible
     * and the node is meant to keep serving.
     *
     * <p>Reversible only counts if something reverses it. The coordinator excludes the OpenSearch
     * node from shard allocation (step 1) before Cassandra gets to refuse (step 2), and an
     * exclusion left in place would outlive the refusal: the node would keep serving what it
     * already has while the cluster never allocated it another shard, so on a single-node cluster
     * every index created afterwards would sit UNASSIGNED forever. The last two assertions are
     * the ones with teeth — the exclusion is gone, and a brand new index goes GREEN.
     */
    @Test
    @Order(6)
    void aSingleNodeDecommissionIsRefusedAndTheNodeKeepsServing() {
        Commands.Result decommission = node.cli("decommission");

        assertThat(decommission.exitCode())
                .as("a single-node decommission must fail:%n%s", decommission.describe())
                .isNotZero();
        assertThat(decommission.output())
                .contains("only member of the ring")
                .contains("--force does not help");

        // Still serving, on both protocols, and still RUNNING as far as the supervisor is concerned.
        assertThat(SupervisorStatus.of(node.cli("status")).value("State")).isEqualTo("RUNNING");
        assertThat(session.execute("SELECT v FROM " + KEYSPACE + ".probe WHERE k = 'k'").one()).isNotNull();
        assertThat(node.rest().post('/' + INDEX + "/_search", """
                {"query":{"match_all":{}}}""").body()).contains("indexed over real HTTP");
        assertThat(node.pidFile())
                .as("the node is still running, so the pid file it was started with must stay")
                .exists();

        Rest.Response settings = node.rest().get("/_cluster/settings?flat_settings=true");
        assertThat(settings.body())
                .as("the refusal must undo the shard-allocation exclusion it set on the way in;"
                        + " left behind, it survives the failed decommission and quietly stops"
                        + " this node from ever being given a shard again:%n%s", settings.body())
                .doesNotContain("cluster.routing.allocation.exclude._name");

        // The assertion that an operator would care about: the cluster still allocates here.
        assertThat(node.rest().put("/it-after-refused-decommission", """
                {"settings":{"number_of_shards":1,"number_of_replicas":0}}""").status()).isEqualTo(200);
        Await.until("the new index turns green", Duration.ofSeconds(60),
                () -> node.rest().cat("/_cat/shards/it-after-refused-decommission?h=state"),
                () -> node.rest().cat("/_cat/shards/it-after-refused-decommission?h=state")
                        .contains("STARTED"));
    }
}
