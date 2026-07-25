/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import io.cassandraopensearch.spi.DecommissionContext;
import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Everything that can be asserted against one running node.
 *
 * <p>The node is started once for the class rather than per test. Booting a real Cassandra takes
 * tens of seconds and leaves an isolated ClassLoader behind that cannot be collected — see
 * {@code docs/spikes/cassandra-embedding.md} — so a node per test would cost minutes and a
 * Cassandra's worth of metaspace each. Teardown is unconditional in {@link #stopNode()}.
 */
class CassandraServiceLifecycleTest {

    @TempDir
    static Path home;

    private static TestNode node;

    @BeforeAll
    static void startNode() throws Exception {
        node = TestNode.started(home);
    }

    @AfterAll
    static void stopNode() throws Exception {
        if (node != null) {
            node.close();
            node = null;
        }
    }

    @Test
    void reportsRunningAndNormal() throws Exception {
        assertThat(node.service().status()).isEqualTo(ServiceStatus.RUNNING);
        assertThat(node.service().status().isServing()).isTrue();

        Map<String, String> details = node.details();
        assertThat(details).containsEntry("operationMode", "NORMAL")
                .containsEntry("joined", "true")
                .containsEntry("gossipActive", "true")
                .containsEntry("nativeTransportActive", "true")
                .containsEntry("clusterName", "cassandra-opensearch-test");
        assertThat(details.get("hostId")).isNotBlank();
        assertThat(details.get("load")).isNotBlank();
        assertThat(details.get("listenAddress")).startsWith(TestNode.ADDRESS + ':');
        assertThat(details.get("nativeAddress")).isEqualTo(TestNode.ADDRESS + ':' + node.nativePort());
        assertThat(details.get("ownership")).contains("100.0%");
        assertThat(details.get("jmx")).contains(String.valueOf(node.jmxPort()));

        // The node logs through its own private logback. A silent NOP binding — what happens if
        // anything downgrades slf4j-api under lib/cassandra/ — would otherwise go unnoticed.
        assertThat(Files.readString(node.logFile())).contains("Startup complete");
    }

    /**
     * The point of this test is not that CQL works — it is that the driver, running in the
     * application ClassLoader with its own Netty, talks to a server whose Netty is a different
     * class in a different loader, over a socket, with nothing shared between them.
     */
    @Test
    void cqlRoundTripFromOutsideTheIsolatedLoader() throws Exception {
        assertThatThrownBy(() -> Class.forName("org.apache.cassandra.service.StorageService"))
                .as("Cassandra must be invisible to the application ClassLoader")
                .isInstanceOf(ClassNotFoundException.class);

        Class<?> driverNetty = Class.forName("io.netty.channel.EventLoopGroup");
        Class<?> serverNetty = Class.forName("io.netty.channel.EventLoopGroup", false, node.isolatedLoader());
        assertThat(serverNetty).isNotSameAs(driverNetty);
        assertThat(serverNetty.getClassLoader()).isSameAs(node.isolatedLoader());

        Row row = cqlRoundTrip("roundtrip");
        assertThat(row.getString("v")).isEqualTo("written-from-the-application-loader");
        assertThat(row.getInt("n")).isEqualTo(42);
    }

    /**
     * What the stock {@code nodetool} does: connect over RMI, read the {@code StorageService}
     * MBean, and immediately build proxies for the platform {@code Memory} and {@code Runtime}
     * MXBeans. The last part is what fails against a bare private MBeanServer, and it is the whole
     * reason {@link FederatedMBeanServer} exists.
     */
    @Test
    @SuppressWarnings("unchecked")
    void stockNodetoolCanManageTheNodeOverJmx() throws Exception {
        try (JMXConnector connector =
                     JMXConnectorFactory.connect(new JMXServiceURL(node.jmxServiceUrl()), null)) {
            MBeanServerConnection connection = connector.getMBeanServerConnection();
            ObjectName storageService = new ObjectName("org.apache.cassandra.db:type=StorageService");

            assertThat(connection.getAttribute(storageService, "OperationMode")).isEqualTo("NORMAL");
            assertThat(connection.getAttribute(storageService, "NativeTransportRunning")).isEqualTo(true);
            assertThat((List<String>) connection.getAttribute(storageService, "LiveNodes"))
                    .contains(TestNode.ADDRESS);

            MemoryMXBean memory = ManagementFactory.newPlatformMXBeanProxy(
                    connection, ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
            assertThat(memory.getHeapMemoryUsage().getUsed()).isPositive();

            RuntimeMXBean runtime = ManagementFactory.newPlatformMXBeanProxy(
                    connection, ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
            assertThat(runtime.getUptime()).isPositive();
        }

        assertThat(ManagementFactory.getPlatformMBeanServer()
                .queryNames(new ObjectName("org.apache.cassandra*:*"), null))
                .as("the node's MBeans must stay out of the JVM platform server that OpenSearch uses")
                .isEmpty();
    }

    /**
     * Regression test for the federation bug. {@code GCInspector} builds its {@code gcStates} map
     * from {@code MBeanWrapper}'s view of {@code java.lang:type=GarbageCollector,*} but listens on
     * the platform server; against a private server the map stays empty and every single GC throws
     * {@code NullPointerException} out of {@code handleNotification}. Recorded statistics are proof
     * that the handler ran to completion.
     */
    @Test
    void gcNotificationsAreHandledWithoutFailing() throws Exception {
        try (JMXConnector connector =
                     JMXConnectorFactory.connect(new JMXServiceURL(node.jmxServiceUrl()), null)) {
            MBeanServerConnection connection = connector.getMBeanServerConnection();

            assertThat(connection.queryNames(new ObjectName("java.lang:type=GarbageCollector,*"), null))
                    .as("the query GCInspector's constructor makes must see the platform GC beans")
                    .isNotEmpty();

            // getAndResetStats() is a getter, so JMX exposes it as the attribute "AndResetStats".
            ObjectName gcInspector = new ObjectName("org.apache.cassandra.service:type=GCInspector");
            connection.getAttribute(gcInspector, "AndResetStats");

            for (int i = 0; i < 3; i++) {
                System.gc();
            }

            double collections = 0;
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (collections == 0 && System.nanoTime() - deadline < 0) {
                Thread.sleep(200);
                double[] stats = (double[]) connection.getAttribute(gcInspector, "AndResetStats");
                collections += stats[5];
            }
            assertThat(collections).as("GCInspector recorded no collection").isGreaterThan(0);
        }

        assertThat(Files.readString(node.logFile()))
                .as("GCInspector logged the empty-gcStates failure")
                .doesNotContain("assumeGCIsPartiallyConcurrent");
    }

    /**
     * {@code StorageService.initServer()} registers a JVM-global drain-on-shutdown hook, and
     * {@code PathUtils} registers more for {@code deleteOnExit}. In a shared process the supervisor
     * owns the only shutdown hook, so none of Cassandra's may survive startup.
     */
    @Test
    void cassandrasJvmShutdownHooksAreRemoved() throws Exception {
        List<String> leftBehind = registeredShutdownHooks().stream()
                .filter(hook -> hook.getName().equals("StorageServiceShutdownHook")
                        || loadedBy(hook.getContextClassLoader(), node.isolatedLoader()))
                .map(Thread::getName)
                .toList();
        assertThat(leftBehind).isEmpty();
    }

    /**
     * {@code JVMStabilityInspector.Killer.killJVM()} calls {@code System.exit(100)} on OOM, FSError
     * and a {@code die} failure policy. It is not covered by {@code CassandraDaemon}'s
     * {@code runManaged} flag, so it has to be replaced outright. If this test ever regresses, the
     * surefire JVM disappears rather than failing.
     */
    @Test
    void simulatedJvmKillIsInterceptedRatherThanExiting() throws Exception {
        Class<?> inspector = Class.forName(
                "org.apache.cassandra.utils.JVMStabilityInspector", true, node.isolatedLoader());
        inspector.getMethod("killCurrentJVM", Throwable.class, boolean.class)
                .invoke(null, new RuntimeException("simulated unrecoverable disk failure"), true);

        assertThat(node.context().fatalErrors()).hasSize(1);
        assertThat(node.context().fatalErrors().get(0).cause())
                .contains("simulated unrecoverable disk failure");
        assertThat(node.service().status()).isEqualTo(ServiceStatus.RUNNING);
        assertThat(node.details().get("interceptedJvmKill"))
                .contains("simulated unrecoverable disk failure");
    }

    /**
     * A one-node ring cannot be decommissioned and {@code --force} does not change that, because
     * Cassandra evaluates the guard before it looks at the flag. The service has to say so plainly
     * instead of hanging or surfacing a bare {@code UnsupportedOperationException}.
     */
    @Test
    void singleNodeDecommissionIsRejectedAndTheNodeKeepsServing() throws Exception {
        DecommissionContext plain = new TestDecommissionContext(Duration.ofSeconds(30), false);
        assertThatThrownBy(() -> node.service().prepareDecommission(plain))
                .isInstanceOf(ServiceException.class)
                .hasMessageContaining("only member of the ring")
                .hasMessageContaining("--force does not help");

        DecommissionContext forced = new TestDecommissionContext(Duration.ofSeconds(30), true);
        assertThatThrownBy(() -> node.service().decommission(forced))
                .isInstanceOf(ServiceException.class)
                .hasMessageContaining("only member of the ring");

        assertThat(node.service().awaitDecommissionReady(plain))
                .as("nothing is in flight, so there is nothing to wait for")
                .isTrue();

        assertThat(node.service().status()).isEqualTo(ServiceStatus.RUNNING);
        assertThat(node.details()).containsEntry("operationMode", "NORMAL");
        assertThat(cqlRoundTrip("after_rejected_decommission").getInt("n")).isEqualTo(42);
    }

    // --- helpers ---------------------------------------------------------------------------

    private static Row cqlRoundTrip(String keyspace) {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(TestNode.ADDRESS, node.nativePort()))
                .withLocalDatacenter("datacenter1")
                .build()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace
                    + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace
                    + ".probe (k text PRIMARY KEY, v text, n int)");
            session.execute("INSERT INTO " + keyspace
                    + ".probe (k, v, n) VALUES ('k','written-from-the-application-loader',42)");
            Row row = session.execute("SELECT k, v, n FROM " + keyspace + ".probe WHERE k = 'k'").one();
            assertThat(row).isNotNull();
            return row;
        }
    }

    @SuppressWarnings("unchecked")
    private static List<Thread> registeredShutdownHooks() throws Exception {
        Class<?> applicationShutdownHooks = Class.forName("java.lang.ApplicationShutdownHooks");
        Field hooks = applicationShutdownHooks.getDeclaredField("hooks");
        hooks.setAccessible(true);
        return List.copyOf(((Map<Thread, Thread>) hooks.get(null)).keySet());
    }

    private static boolean loadedBy(ClassLoader candidate, ClassLoader ancestor) {
        for (ClassLoader loader = candidate; loader != null; loader = loader.getParent()) {
            if (loader == ancestor) {
                return true;
            }
        }
        return false;
    }
}
