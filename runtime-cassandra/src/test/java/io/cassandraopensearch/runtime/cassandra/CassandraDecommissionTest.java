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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

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
     * The operation mode does not say whether the node left, and this is the test that holds the
     * two halves of that against each other: one node, one mode string —
     * {@code DECOMMISSION_FAILED} — and opposite conclusions, because ring membership differs.
     *
     * <p>The fork sets that mode from catch blocks that wrap the <i>whole</i> body of
     * {@code StorageService.decommission()}, and the body begins long before {@code unbootstrap()}
     * calls {@code leaveRing()}. The interrupt below lands in the {@code RING_DELAY} sleep between
     * {@code startLeaving()} and {@code unbootstrap()}: nothing has streamed, no range has moved,
     * and the node is still in {@code TokenMetadata} serving every one of them. Calling that FAILED
     * costs the operator the process — the supervisor's health monitor turns FAILED into
     * {@code onFatalError} and exits 1 — and blocks the retry the fork explicitly re-admits from
     * this very mode.
     *
     * <p>The second half fakes what {@code leaveRing()} does to the token metadata and asks again.
     * Same mode, and now the node really has left, so DECOMMISSIONED is the answer: reporting
     * RUNNING there would tell the operator this node is still serving a ring it is out of.
     */
    @Test
    void whatAFailedDecommissionReportsFollowsRingMembershipNotTheModeString(@TempDir Path home)
            throws Exception {
        node = TestNode.started(home);
        Cassandra cassandra = new Cassandra(node.isolatedLoader());
        cassandra.dontStreamHintsOnDecommission();
        cassandra.pretendThisNodeIsAlreadyLeaving();

        // decommission() blocks for the whole procedure, so the interrupt has to come from
        // somewhere else. The window is the fork's own `sleep(max(ring_delay, batchlog_timeout))`
        // between startLeaving() and unbootstrap(), which is 5s here (cassandra.ring_delay_ms).
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread decommissioning = new Thread(() -> {
            try {
                node.service().decommission(new TestDecommissionContext(Duration.ofMinutes(5), false));
            } catch (Throwable failure) {
                thrown.set(failure);
            }
        }, "decommission-under-test");
        decommissioning.start();
        Thread.sleep(1500);
        decommissioning.interrupt();
        decommissioning.join(Duration.ofMinutes(2).toMillis());
        assertThat(decommissioning.isAlive()).isFalse();

        assertThat(node.details())
                .as("the fork records the abandoned attempt in the operation mode")
                .containsEntry("operationMode", "DECOMMISSION_FAILED")
                .as("and it never got as far as leaveRing(), so the ranges are all still here")
                .containsEntry("joined", "true");
        assertThat(node.service().status())
                .as("the node holds every range it held before and is still serving them;"
                        + " FAILED here is what the health monitor turns into an exit(1)")
                .isEqualTo(ServiceStatus.RUNNING);
        assertThat(thrown.get())
                .isInstanceOf(ServiceException.class)
                .hasMessageContaining("still a member of the ring");

        // Now the other half: the same mode, on a node that really has left the ring.
        cassandra.pretendThisNodeHasLeftTheRing();

        assertThatThrownBy(() -> node.service().decommission(
                new TestDecommissionContext(Duration.ofMinutes(5), false)))
                .isInstanceOf(ServiceException.class)
                .hasMessageContaining("already left the ring");

        assertThat(node.details())
                .as("unchanged; it is the ring membership below that decides, not this")
                .containsEntry("operationMode", "DECOMMISSION_FAILED")
                .containsEntry("joined", "false");
        assertThat(node.service().status())
                .as("out of the ring: RUNNING would say it is still serving one it has left")
                .isEqualTo(ServiceStatus.DECOMMISSIONED);

        node.service().stop();

        assertThat(node.service().status())
                .as("a node that left is not one that merely stopped; stop() must not erase it")
                .isEqualTo(ServiceStatus.DECOMMISSIONED);
        assertThat(node.details()).containsEntry("status", "DECOMMISSIONED");
    }

    /**
     * The refusal an operator meets on an ordinary cluster, and the one the round-3 mapping turned
     * into a process kill.
     *
     * <p>{@code system_distributed} is RF 3 by default, so on a ring of fewer than four nodes
     * {@code StorageService.decommission(force=false)} throws {@code UnsupportedOperationException}
     * — "Not enough live nodes to maintain replication factor … Perform a forceful decommission to
     * ignore" — from inside the try that sets {@code DECOMMISSION_FAILED}, having done nothing at
     * all. {@code examples/local-cluster/cluster-control.sh} documents this as the normal state of
     * a two- or three-node cluster and passes {@code --force} for it.
     *
     * <p>So the node must still be RUNNING: it never left, the supervisor puts its own state back
     * to RUNNING so the operator can retry, and a service reporting FAILED at that point takes the
     * whole process down through the health monitor. The retry has to be admitted too — the fork
     * re-admits a decommission whose mode is {@code DECOMMISSION_FAILED}, and
     * {@code requireRunning} would refuse it for any other status.
     */
    @Test
    void aDecommissionRefusedOverTheReplicationFactorLeavesTheNodeRunningAndRetryable(
            @TempDir Path home) throws Exception {
        node = TestNode.started(home);
        Cassandra cassandra = new Cassandra(node.isolatedLoader());
        // Two endpoints, which is what gets past both this service's sole-member guard and the
        // fork's, and is still below the RF of system_distributed.
        cassandra.pretendAnotherNodeIsInTheRing("127.0.0.9");

        Throwable refusal = catchThrowable(() -> node.service().decommission(
                new TestDecommissionContext(Duration.ofMinutes(5), false)));

        assertThat(node.details())
                .containsEntry("operationMode", "DECOMMISSION_FAILED")
                .as("the refusal is evaluated before anything moves")
                .containsEntry("joined", "true");
        assertThat(node.service().status())
                .as("a refused decommission is not a broken node")
                .isEqualTo(ServiceStatus.RUNNING);
        assertThat(refusal)
                .isInstanceOf(ServiceException.class)
                .hasMessageContaining("still a member of the ring")
                .hasStackTraceContaining("Not enough live nodes to maintain replication factor");

        // And the retry the fork allows reaches Cassandra rather than being refused here.
        assertThatThrownBy(() -> node.service().decommission(
                new TestDecommissionContext(Duration.ofMinutes(5), false)))
                .isInstanceOf(ServiceException.class)
                .hasMessageNotContaining("requires a running node")
                .hasStackTraceContaining("Not enough live nodes to maintain replication factor");

        node.service().stop();

        assertThat(node.service().status())
                .as("nothing failed and nothing left; this node was stopped")
                .isEqualTo(ServiceStatus.STOPPED);
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
     * The same leak, on the failure that happens <i>before</i> a socket is ever asked for.
     *
     * <p>{@code NodeJmxServer.start()} touches {@code MBeanWrapper.instance} — which is what
     * creates the private {@code MBeanServer} — and only then resolves the bind address and builds
     * the RMI socket factory. A {@code cassandra.jmx.address} that does not resolve throws
     * {@code UnknownHostException} from {@code InetAddress.getByName} at that point, which is two
     * statements earlier than a port collision throws, and a compensation boundary drawn between
     * the two catches one and leaks the other. Nothing else ever comes back for it: {@code stop()}
     * takes the {@code daemon == null} / {@code jmx == null} branch, and {@code MBeanServerFactory}
     * holds the server — and through it the isolated ClassLoader — for the life of the process.
     *
     * <p>{@code failedSurvivesStopAndTakesNoProcessWideStateWithIt} above covers the other side of
     * the same boundary, and passed throughout the whole time this one did not.
     */
    @Test
    void aJmxAddressThatDoesNotResolveLeavesNoMBeanServerBehind(@TempDir Path home) throws Exception {
        // NodeJmxServer pins java.rmi.server.hostname from this value before anything can fail, and
        // that property is JVM-global and cached by RMI on first use. Put it back, so that a node
        // started later in this JVM is not handed "not a host name".
        String rmiHostname = System.getProperty("java.rmi.server.hostname");
        try {
            // Spaces: no resolver anywhere will turn this into an address, whereas a name in a
            // reserved TLD is at the mercy of a provider that answers NXDOMAIN with an ad server.
            node = TestNode.create(home, "not a host name");
            int mbeanServersBefore = privateMBeanServers();

            // Not hasRootCauseInstanceOf: on JDK 21 the platform resolver rethrows the
            // UnknownHostException wrapped in a RuntimeException that does not chain it, so the
            // type is only in the message.
            assertThatThrownBy(() -> node.service().start(node.context()))
                    .isInstanceOf(ServiceException.class)
                    .hasStackTraceContaining("UnknownHostException");

            assertThat(node.service().status()).isEqualTo(ServiceStatus.FAILED);

            node.service().stop();

            assertThat(privateMBeanServers())
                    .as("the private MBeanServer already existed when getByName threw; releasing it"
                            + " is the only thing that lets the isolated ClassLoader be collected")
                    .isEqualTo(mbeanServersBefore);
        } finally {
            if (rmiHostname == null) {
                System.clearProperty("java.rmi.server.hostname");
            } else {
                System.setProperty("java.rmi.server.hostname", rmiHostname);
            }
        }
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

        /**
         * Puts a second endpoint in the token metadata, with a token, exactly as gossip would on
         * a node that has a peer.
         *
         * <p>Two endpoints is the smallest ring in which a decommission is neither refused as
         * pointless — by {@code rejectSoleRingMember} here or by the fork's own equivalent — nor
         * allowed unforced, since {@code system_distributed} is RF 3. That is the shape of every
         * two- and three-node cluster, which is what makes the refusal it produces routine.
         */
        void pretendAnotherNodeIsInTheRing(String address) throws Exception {
            Class<?> endpointType = load("org.apache.cassandra.locator.InetAddressAndPort");
            Object peer = endpointType.getMethod("getByName", String.class).invoke(null, address);
            Object tokenMetadata = storageServiceType.getMethod("getTokenMetadata").invoke(storageService);
            Object partitioner = tokenMetadata.getClass().getField("partitioner").get(tokenMetadata);
            Object token = partitioner.getClass().getMethod("getRandomToken").invoke(partitioner);
            Class<?> tokenType = load("org.apache.cassandra.dht.Token");
            tokenMetadata.getClass().getMethod("updateNormalToken", tokenType, endpointType)
                    .invoke(tokenMetadata, token, peer);
            // Both halves, because they are different maps and both are read here: the tokens
            // decide who owns what, and getAllEndpoints() — which is what every "how many nodes
            // are there" check in the decommission path calls — reads the host id map.
            tokenMetadata.getClass().getMethod("updateHostId", UUID.class, endpointType)
                    .invoke(tokenMetadata, UUID.randomUUID(), peer);
        }

        /**
         * What {@code leaveRing()} does to the token metadata, and only that: take this endpoint
         * out of it. The mode is deliberately left alone — the point of the test that calls this
         * is that the mode string is the same on both sides of the departure.
         */
        void pretendThisNodeHasLeftTheRing() throws Exception {
            Class<?> fbUtilities = load("org.apache.cassandra.utils.FBUtilities");
            Object localEndpoint = fbUtilities.getMethod("getBroadcastAddressAndPort").invoke(null);
            Class<?> endpointType = load("org.apache.cassandra.locator.InetAddressAndPort");
            Object tokenMetadata = storageServiceType.getMethod("getTokenMetadata").invoke(storageService);
            tokenMetadata.getClass().getMethod("removeEndpoint", endpointType)
                    .invoke(tokenMetadata, localEndpoint);
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
