/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the service reports once leaving the ring has gone one way or the other. Every test here
 * destroys the node it uses, so each one boots its own.
 */
class CassandraDecommissionTest {

    private TestNode node;

    @AfterEach
    void tearDown() throws Exception {
        if (node != null) {
            node.close();
            node = null;
        }
    }

    /**
     * A decommission that throws has not necessarily stopped short.
     *
     * <p>This fork runs its decommission hooks <i>after</i> {@code unbootstrap()} and then
     * finishes the whole sequence regardless — {@code leaveRing}, {@code shutdownClientServers},
     * {@code Gossiper.stop}, {@code MessagingService.shutdown}, {@code Stage.shutdownNow},
     * {@code setMode(DECOMMISSIONED)} — before throwing to report the failed hook, deliberately,
     * because by then there is nothing left to roll back to. Concluding "it threw, so the node is
     * still serving" and reporting RUNNING tells the operator this node is a member of a ring it
     * has already left, and leaves the supervisor believing it still has somewhere to route.
     *
     * <p>The second half is the sibling of {@code OpenSearchDecommissionTest}'s
     * {@code decommissionedSurvivesStop}: DECOMMISSIONED is terminal and successful, and the
     * supervisor reports the outcome after every service has been stopped, so {@code stop()} must
     * not overwrite it with STOPPED. Asserted here rather than in its own test because reaching
     * DECOMMISSIONED at all costs a node.
     */
    @Test
    void aDecommissionThatLeftTheRingIsNotReportedAsRunning(@TempDir Path home) throws Exception {
        node = TestNode.started(home);
        Cassandra cassandra = new Cassandra(node.isolatedLoader());
        cassandra.registerFailingDecommissionHook("test-hook-that-fails");
        cassandra.dontStreamHintsOnDecommission();
        cassandra.pretendThisNodeIsAlreadyLeaving();

        assertThatThrownBy(() -> node.service().decommission(
                new TestDecommissionContext(Duration.ofMinutes(5), false)))
                .isInstanceOf(ServiceException.class)
                .hasMessageContaining("already left the ring");

        assertThat(node.service().status())
                .as("the node is out of the ring; only the hook failed")
                .isEqualTo(ServiceStatus.DECOMMISSIONED);
        assertThat(node.details()).containsEntry("operationMode", "DECOMMISSIONED");

        node.service().stop();

        assertThat(node.service().status())
                .as("DECOMMISSIONED is terminal and successful; stop() must not erase it")
                .isEqualTo(ServiceStatus.DECOMMISSIONED);
        assertThat(node.details()).containsEntry("status", "DECOMMISSIONED");
    }

    /**
     * The other state {@code stop()} must not erase, on the path that produces it: a start that
     * failed before the daemon existed. FAILED is the post-mortem — it says the node died rather
     * than being asked to leave — and the supervisor reads it after the final stop.
     *
     * <p>Doubles as the check on the two pieces of process-wide state a failed start must give
     * back. {@code MBeanServerFactory} keeps every server it creates in a JVM-global list until it
     * is explicitly released, and touching {@code MBeanWrapper.instance} is what creates this
     * node's private one — so a {@code NodeJmxServer.start()} that fails after that point, as it
     * does here on a port already in use, would otherwise pin the isolated ClassLoader for the
     * life of the process. The default uncaught-exception handler is JVM-global for the same
     * reason.
     */
    @Test
    void failedSurvivesStopAndTakesNoProcessWideStateWithIt(@TempDir Path home) throws Exception {
        node = TestNode.create(home);
        int mbeanServersBefore = privateMBeanServers();
        Thread.UncaughtExceptionHandler handlerBefore = Thread.getDefaultUncaughtExceptionHandler();

        // The JMX bring-up is the first thing in start() that can fail, and it runs before the
        // daemon is built — so this is the daemon == null path through stop().
        try (ServerSocket blocked = new ServerSocket(
                node.jmxPort(), 1, InetAddress.getByName(TestNode.ADDRESS))) {
            assertThat(blocked.isBound()).isTrue();
            assertThatThrownBy(() -> node.service().start(node.context()))
                    .isInstanceOf(ServiceException.class);
        }

        assertThat(node.service().status()).isEqualTo(ServiceStatus.FAILED);

        node.service().stop();

        assertThat(node.service().status())
                .as("a node that failed did not merely stop, and the supervisor reports the"
                        + " outcome after the final stop")
                .isEqualTo(ServiceStatus.FAILED);
        assertThat(node.details()).containsEntry("status", "FAILED");
        assertThat(privateMBeanServers())
                .as("MBeanServerFactory holds every server it creates until it is released;"
                        + " an unreleased one pins the isolated ClassLoader for the whole process")
                .isEqualTo(mbeanServersBefore);
        assertThat(Thread.getDefaultUncaughtExceptionHandler())
                .as("nothing of Cassandra's may be left installed process-wide")
                .isSameAs(handlerBefore);
    }

    /**
     * The window between {@code CassandraDaemon.setup()} taking the process-wide uncaught-exception
     * handler and this service wrapping it.
     *
     * <p>{@code setup()} installs {@code JVMStabilityInspector::uncaughtException} as the JVM's
     * default handler roughly half way through, and the wrapper that remembers what it displaced
     * is installed only once {@code activate()} has returned. A failure in between — here the
     * native transport finding its port taken, which is the last thing {@code activate()} does —
     * leaves Cassandra's handler installed for the whole process with nothing that knows how to
     * take it back off, pinning the isolated ClassLoader it came from.
     *
     * <p>The drain-on-shutdown hook {@code StorageService.initServer()} registers with the JVM is
     * in the same window and goes the same way: {@code removeShutdownHooks()} runs only once
     * {@code activate()} has returned. Left registered it runs on the supervisor's exit, out of
     * order, against a node that never came up.
     */
    @Test
    void aStartThatFailedAfterCassandraTookTheUncaughtExceptionHandlerGivesItBack(@TempDir Path home)
            throws Exception {
        node = TestNode.create(home);
        Thread.UncaughtExceptionHandler handlerBefore = Thread.getDefaultUncaughtExceptionHandler();

        try (ServerSocket blocked = new ServerSocket(
                node.nativePort(), 1, InetAddress.getByName(TestNode.ADDRESS))) {
            assertThat(blocked.isBound()).isTrue();
            assertThatThrownBy(() -> node.service().start(node.context()))
                    .isInstanceOf(ServiceException.class);
        }

        assertThat(node.service().status()).isEqualTo(ServiceStatus.FAILED);
        assertThat(Thread.getDefaultUncaughtExceptionHandler())
                .as("Cassandra's handler outlives its node and pins its ClassLoader")
                .isSameAs(handlerBefore);
        assertThat(registeredShutdownHookNames())
                .as("in a shared process the supervisor owns the only shutdown hook")
                .doesNotContain("StorageServiceShutdownHook");
    }

    @SuppressWarnings("unchecked")
    private static List<String> registeredShutdownHookNames() throws Exception {
        Field hooks = Class.forName("java.lang.ApplicationShutdownHooks").getDeclaredField("hooks");
        hooks.setAccessible(true);
        return ((Map<Thread, Thread>) hooks.get(null)).keySet().stream().map(Thread::getName).toList();
    }

    /** How many of this node's private MBean servers the JVM is still holding. */
    private static int privateMBeanServers() {
        int found = 0;
        for (MBeanServer server : MBeanServerFactory.findMBeanServer(null)) {
            if (NodeMBeanWrapper.DEFAULT_DOMAIN.equals(server.getDefaultDomain())) {
                found++;
            }
        }
        return found;
    }

    /**
     * Reaches into the running node the way {@code CassandraServiceLifecycleTest} does. Nothing
     * here is visible to the application ClassLoader — that is the point of the isolation — so
     * everything goes through the isolated loader by name.
     */
    private static final class Cassandra {

        private final ClassLoader loader;
        private final Class<?> storageServiceType;
        private final Object storageService;

        Cassandra(ClassLoader loader) throws Exception {
            this.loader = loader;
            this.storageServiceType = load("org.apache.cassandra.service.StorageService");
            this.storageService = storageServiceType.getField("instance").get(null);
        }

        /**
         * A hook that throws. Hooks are the one thing that fails a decommission <i>after</i> the
         * node has already left the ring, which is the whole point of the test; a
         * {@link Proxy} is how a test outside the isolated loader can implement an interface that
         * only exists inside it.
         */
        void registerFailingDecommissionHook(String name) throws Exception {
            Class<?> hookType = load("org.apache.cassandra.service.DecommissionHook");
            InvocationHandler handler = (proxy, method, args) -> {
                switch (method.getName()) {
                    case "name":
                        return name;
                    case "onDecommission":
                        throw new IllegalStateException("simulated decommission hook failure");
                    case "toString":
                        return name;
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "equals":
                        return proxy == args[0];
                    default:
                        throw new UnsupportedOperationException(method.getName());
                }
            };
            Object hook = Proxy.newProxyInstance(loader, new Class<?>[]{hookType}, handler);
            storageServiceType.getMethod("registerDecommissionHook", hookType)
                    .invoke(storageService, hook);
        }

        /**
         * Hint streaming picks the closest live peer and fails outright when there is none, which
         * on a single node would end the decommission before it ever reached the hooks. Switching
         * it off is a supported runtime setting and takes the shipped alternative branch: pause
         * dispatch and delete the hints.
         */
        void dontStreamHintsOnDecommission() throws Exception {
            storageServiceType.getMethod("setTransferHintsOnDecommission", boolean.class)
                    .invoke(storageService, false);
        }

        /**
         * Puts the node in the state a {@code nodetool decommission} that has already announced
         * itself would be in: LEAVING, and marked as leaving in the token metadata.
         *
         * <p>That is what makes the rest reachable on a one-node ring. Both {@code
         * StorageService.decommission} and the service's own {@code rejectSoleRingMember} refuse a
         * node that is the last member — the former skips those guards entirely once the mode is
         * LEAVING, the latter is satisfied because a leaving endpoint is not in the
         * {@code cloneAfterAllLeft()} view. Everything after the guards is the real thing: real
         * ranges (there are none to move), a real {@code leaveRing()}, real hooks, and a real
         * shutdown into DECOMMISSIONED.
         */
        void pretendThisNodeIsAlreadyLeaving() throws Exception {
            Class<?> fbUtilities = load("org.apache.cassandra.utils.FBUtilities");
            Object localEndpoint = fbUtilities.getMethod("getBroadcastAddressAndPort").invoke(null);
            Class<?> endpointType = load("org.apache.cassandra.locator.InetAddressAndPort");
            Object tokenMetadata = storageServiceType.getMethod("getTokenMetadata").invoke(storageService);
            tokenMetadata.getClass().getMethod("addLeavingEndpoint", endpointType)
                    .invoke(tokenMetadata, localEndpoint);
            setMode("LEAVING");
        }

        /** {@code setMode} is private; it assigns a field and logs, and has no other effect. */
        private void setMode(String mode) throws Exception {
            Class<?> modeEnum = load("org.apache.cassandra.service.StorageService$Mode");
            Method setMode = storageServiceType.getDeclaredMethod("setMode", modeEnum, boolean.class);
            setMode.setAccessible(true);
            setMode.invoke(storageService,
                    modeEnum.getMethod("valueOf", String.class).invoke(null, mode), true);
        }

        private Class<?> load(String name) throws Exception {
            return Class.forName(name, true, loader);
        }
    }
}
