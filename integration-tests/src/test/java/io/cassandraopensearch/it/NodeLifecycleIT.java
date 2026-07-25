/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import io.cassandraopensearch.server.config.NodeConfiguration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code start -d}, {@code status}, {@code stop} — the whole life of a node, driven the way an
 * operator drives it.
 *
 * <p>Ordered, and sharing one node, because the assertions are about a sequence: the pid file
 * exists <i>while</i> the node runs and is gone <i>after</i> it stops, and {@code status} means
 * something different on each side of the stop. A node per test would also cost a cold Cassandra
 * boot per test for no additional coverage.
 *
 * <p>The shutdown assertions are the ones with teeth. {@code bin/cassandra-opensearch stop} sends
 * SIGTERM and then waits; it never escalates to SIGKILL, and neither does this test. If a
 * non-daemon thread outlived the ordered shutdown the process would still be there at the
 * deadline, and {@link DistributionNode#wasKilled()} would say so.
 */
@ExtendWith(DumpLogsOnFailure.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NodeLifecycleIT {

    private static DistributionNode node;

    @BeforeAll
    static void startNode() {
        node = DistributionNode.install(
                        Distribution.workDirectory(NodeLifecycleIT.class, "node"),
                        "lifecycle", NodeEndpoints.FIRST, ClusterTopology.alone(NodeEndpoints.FIRST))
                .start();
    }

    @AfterAll
    static void tearDown() {
        if (node != null) {
            node.close();
        }
    }

    @Test
    @Order(1)
    void statusReportsBothServicesRunning() {
        SupervisorStatus status = SupervisorStatus.of(node.cli("status"));

        assertThat(status.value("State")).isEqualTo("RUNNING");
        assertThat(status.value("ServiceStatuses"))
                .contains("cassandra=RUNNING")
                .contains("opensearch=RUNNING");

        // The supervisor reporting RUNNING is not the same claim as either server being usable:
        // these two come from StorageService and from the applied cluster state.
        assertThat(status.value("operationMode")).isEqualTo("NORMAL");
        assertThat(status.value("cluster.health")).isIn("GREEN", "YELLOW");
        assertThat(status.value("nativeTransportActive")).isEqualTo("true");
    }

    /**
     * The supervisor is parsing the file the tarball ships, at the path the tarball puts it.
     *
     * <p>{@code ShippedConfigurationTest} in the server module parses
     * {@code dist/src/main/resources/conf/cassandra-opensearch.yaml} — the source-tree copy. That
     * proves the file is valid; it cannot prove the assembly put it in the archive, that the
     * archive puts it where the launcher looks, or that the running process took its ports from
     * it. This loads the archive's copy with the supervisor's own parser and checks the answers
     * against what the live process reports over JMX.
     */
    @Test
    @Order(2)
    void theShippedConfigurationIsTheOneTheSupervisorParses() throws Exception {
        Path pristine = Distribution.extract("conf/cassandra-opensearch.yaml",
                Distribution.workDirectory(NodeLifecycleIT.class, "shipped-conf"));
        NodeConfiguration shipped = NodeConfiguration.load(pristine.getParent().getParent(), pristine);

        assertThat(shipped.clusterName()).isEqualTo("cassandra-opensearch");
        assertThat(shipped.service("opensearch").settings())
                .containsEntry("http_port", "9200")
                .containsEntry("transport_port", "9300");
        assertThat(shipped.service("cassandra").settings()).containsEntry("jmx.port", "7199");

        // And now the installed copy, which this test rewrote to keep off the stock ports, read
        // by the same parser and compared with what the process is actually doing.
        NodeConfiguration installed = NodeConfiguration.load(node.home());
        SupervisorStatus status = SupervisorStatus.of(node.cli("status"));

        assertThat(status.value("ClusterName")).isEqualTo(installed.clusterName());
        assertThat(status.port("http.address"))
                .isEqualTo(Integer.parseInt(installed.service("opensearch").settings().get("http_port")))
                .isEqualTo(node.endpoints().httpPort());
        assertThat(status.value("jmx"))
                .contains(':' + installed.service("cassandra").settings().get("jmx.port"));
        assertThat(installed.dataDirectory()).isEqualTo(node.home().resolve("data"));
        assertThat(Files.isDirectory(installed.dataDirectory().resolve("cassandra"))).isTrue();
        assertThat(Files.isDirectory(installed.dataDirectory().resolve("opensearch"))).isTrue();
    }

    @Test
    @Order(3)
    void thePidFileNamesTheLiveProcess() {
        assertThat(node.readPidFile()).contains(node.pid());
        assertThat(ProcessHandle.of(node.pid()).orElseThrow().info().commandLine().orElseThrow())
                .as("the pid file must name the supervisor JVM, not the launcher script")
                .contains("io.cassandraopensearch.server.CassandraOpenSearchServer");
    }

    /**
     * Stops the node and holds the shutdown to its contract: OpenSearch first, then Cassandra,
     * then a process that exits by itself.
     */
    @Test
    @Order(4)
    void stopShutsDownInOrderAndTheProcessExitsOnItsOwn() {
        Commands.Result stop = node.stop();

        assertThat(stop.succeeded()).as("stop failed:%n%s", stop.describe()).isTrue();
        assertThat(stop.output()).contains("stopped");
        node.awaitExit(Duration.ofMinutes(2));
        assertThat(node.wasKilled()).as("the process had to be killed; it did not exit on its own").isFalse();
        assertThat(node.pidFile()).doesNotExist();

        String log = node.supervisorLog();
        assertThat(log).contains("Shutting down", "Shutdown complete; exit code 0");
        assertThat(log.indexOf("Stopping service 'opensearch'"))
                .as("OpenSearch is the dependent service and must go down first")
                .isGreaterThan(-1)
                .isLessThan(log.indexOf("Stopping service 'cassandra'"));
    }

    @Test
    @Order(5)
    void statusAfterStopExitsNonZeroAndSaysWhy() {
        Commands.Result status = node.cli("status");

        // 3 is the CLI's documented "nothing is running", which is what the launcher's start loop
        // distinguishes from a real failure.
        assertThat(status.exitCode()).isEqualTo(3);
        assertThat(status.output()).contains("Cannot reach a cassandra-opensearch process");
    }

    /**
     * Every port is bindable again, which is the observable half of "the process really exited".
     * A supervisor that left a listening socket behind would let the next {@code start} come up
     * half-broken instead of failing.
     */
    @Test
    @Order(6)
    void theNodeReleasedEveryPortItHeld() throws Exception {
        assertThat(node.isAlive()).isFalse();
        NodeEndpoints endpoints = node.endpoints();
        for (int port : new int[] {endpoints.nativePort(), endpoints.storagePort(),
                endpoints.httpPort(), endpoints.transportPort(),
                endpoints.cassandraJmxPort(), endpoints.supervisorJmxPort()}) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress(endpoints.host(), port));
            } catch (IOException e) {
                throw new AssertionError("port " + port + " is still held after stop", e);
            }
        }
    }
}
