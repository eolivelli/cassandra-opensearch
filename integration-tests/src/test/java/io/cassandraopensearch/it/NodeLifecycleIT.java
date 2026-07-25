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
import java.util.Map;

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
     * A {@code start} with no {@code conf/jvm21-server.options} must fail before it starts a JVM.
     *
     * <p>The guard that says so lives in {@code co_jvm_options}, which every caller invokes inside
     * a command substitution — so its {@code exit 1} ends the substitution's subshell and nothing
     * else. The launcher used to print the error and then start the JVM anyway, with no
     * {@code --add-exports} and no {@code --add-opens}, which dies deep in startup with
     * {@code IllegalAccessError: module java.rmi does not export sun.rmi.registry}.
     *
     * <p>A conf/ directory mounted over the shipped one — which is what the Dockerfile tells an
     * operator to do to change the bind addresses — is how an installation arrives in this state.
     *
     * <p>{@code start -d} with a pid file of its own: {@code -d} never execs, so a regression
     * fails this test instead of hanging it, and the running node's own pid file is untouched
     * either way.
     */
    @Test
    @Order(4)
    void startWithoutTheJvmOptionsFileFailsInsteadOfStartingACrippledJvm() throws Exception {
        Path emptyConf = Files.createDirectories(
                Distribution.workDirectory(NodeLifecycleIT.class, "conf-without-options"));
        Path pidFile = node.home().resolve("guard.pid");
        Files.deleteIfExists(pidFile);

        Commands.Result start = node.runWith(
                Map.of("CASSANDRA_OPENSEARCH_CONF", emptyConf.toString()),
                Duration.ofMinutes(2),
                "bin/cassandra-opensearch", "start", "-d", "-p", pidFile.toString());

        assertThat(start.exitCode()).as("start must fail:%n%s", start.describe()).isNotZero();
        assertThat(start.output())
                .as("the failure has to name the file that is missing")
                .contains("jvm21-server.options");
        assertThat(start.output())
                .as("no JVM may be launched:%n%s", start.describe())
                .doesNotContain("started pid");
        assertThat(pidFile).doesNotExist();

        // And the tools take the same path, through bin/cassandra.in.sh rather than the launcher.
        Commands.Result nodetool = node.runWith(
                Map.of("CASSANDRA_OPENSEARCH_CONF", emptyConf.toString()),
                Duration.ofMinutes(2), "bin/nodetool", "status");
        assertThat(nodetool.exitCode()).as("nodetool must fail:%n%s", nodetool.describe()).isNotZero();
        assertThat(nodetool.output()).contains("jvm21-server.options");
        // What a JVM started without those entries prints on its way down. The message above
        // names IllegalAccessError to explain itself, so the assertion has to be on the symptom
        // rather than on the word.
        assertThat(nodetool.output())
                .as("it must not have got as far as the JVM")
                .doesNotContain("unable to access required classes")
                .doesNotContain("java.lang.IllegalAccessException");
    }

    /**
     * A garbage pid file must not let a second {@code start -d} report success.
     *
     * <p>The already-running guard only fired when the pid file existed <i>and</i> parsed.
     * Otherwise a second JVM launched, died on {@code BindException} — and the wait loop, polling
     * the same JMX host and port, connected to the node that was <b>already</b> running and
     * printed "up" before {@code kill -0} noticed the new process was gone. The pid file then
     * named a dead process, and {@code stop} refused to touch the live one: a running node with
     * no way to stop it through the script.
     *
     * <p>This node is up while the test runs, which is the whole point — the false "up" needs
     * something real answering on that port.
     */
    @Test
    @Order(5)
    void aGarbagePidFileDoesNotProduceAFalseUp() throws Exception {
        Path stalePidFile = node.home().resolve("stale.pid");
        Files.writeString(stalePidFile, "not-a-pid\n");

        Commands.Result start = node.runWith(Map.of(), Duration.ofMinutes(3),
                "bin/cassandra-opensearch", "start", "-d", "-p", stalePidFile.toString());

        assertThat(start.exitCode())
                .as("a second node on the same JMX endpoint must be refused:%n%s", start.describe())
                .isNotZero();
        assertThat(start.output())
                .as("it must not claim the node came up:%n%s", start.describe())
                .doesNotContain("up; supervisor JMX");
        assertThat(start.output()).contains("already answering");

        // Nothing was launched, so the file it was told to write still holds what we put there.
        assertThat(Files.readString(stalePidFile).trim()).isEqualTo("not-a-pid");
        // And the real node is untouched: same pid, still alive, still answering.
        assertThat(node.readPidFile()).contains(node.pid());
        assertThat(node.isAlive()).isTrue();
        assertThat(node.cli("status").exitCode()).isZero();

        Files.deleteIfExists(stalePidFile);
    }

    /**
     * The same refusal with a pid file that does not exist at all, which is the other half of the
     * blind spot: there is then nothing to parse and nothing to {@code kill -0}, and the JMX port
     * is the only thing that knows a node is already there.
     */
    @Test
    @Order(6)
    void startIsRefusedWhileTheNodeIsAlreadyAnswering() throws Exception {
        Path absentPidFile = node.home().resolve("absent.pid");
        Files.deleteIfExists(absentPidFile);

        Commands.Result start = node.runWith(Map.of(), Duration.ofMinutes(3),
                "bin/cassandra-opensearch", "start", "-d", "-p", absentPidFile.toString());

        assertThat(start.exitCode()).as("start must be refused:%n%s", start.describe()).isNotZero();
        assertThat(start.output()).contains("already answering");
        assertThat(absentPidFile).doesNotExist();
        assertThat(node.isAlive()).isTrue();
    }

    /**
     * Stops the node and holds the shutdown to its contract: OpenSearch first, then Cassandra,
     * then a process that exits by itself.
     */
    @Test
    @Order(7)
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
    @Order(8)
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
    @Order(9)
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
