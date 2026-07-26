/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The two pieces of JVM-global state {@link NodeMBeanWrapper} takes, and what it does with them
 * when its own cleanup fails.
 *
 * <p>Both are the same mistake: mark the work done, then do the work. An idempotence flag set
 * before the thing it guards makes the retry that the rest of the class relies on unreachable, and
 * what is left behind — a notification listener on the platform garbage-collector MXBeans, an
 * {@code MBeanServer} in {@code MBeanServerFactory}'s list — is a hard reference to the isolated
 * ClassLoader for the life of the process, which is the one thing this module exists to prevent.
 *
 * <p>No node is started. The wrapper is Cassandra's extension point and Cassandra constructs it,
 * but it needs nothing from a running one: an isolated loader is enough, and starting a node here
 * would cost a minute to test a field assignment.
 */
class NodeMBeanWrapperTest {

    private static final String WRAPPER_CLASS =
            "io.cassandraopensearch.runtime.cassandra.NodeMBeanWrapper";

    /** Cassandra's extension point, which {@code CassandraService.start()} sets JVM-wide. */
    private static final String REGISTRATION_PROPERTY =
            "org.apache.cassandra.mbean_registration_class";

    private TestNode node;

    @AfterEach
    void tearDown() throws Exception {
        if (node != null) {
            node.close();
            node = null;
        }
    }

    /**
     * A detach that could not reach the platform beans must leave the inspector where {@code
     * close()} can find it.
     *
     * <p>{@code CassandraService.stop()} runs {@code detachGcInspector()} as its very first step,
     * before {@code drain()}, and {@code close()} runs it again for the paths that never get that
     * far — the second call is the compensation the first one's failure is supposed to reach.
     * Clearing the reference on the way in makes that retry a no-op: the listener stays subscribed
     * to a JVM-global MXBean with nothing left that knows which object to unsubscribe, so the
     * loader can never be collected and every later collection notification still runs code from
     * a node that no longer exists.
     */
    @Test
    void aDetachThatFailedLeavesSomethingForTheRetryToDo(@TempDir Path home) throws Exception {
        node = TestNode.create(home);
        // The successful detach at the end of this test logs, and logback resolves the node's log
        // file from this property. Only start() normally sets it, and no node is started here.
        Path logs = home.resolve("logs");
        Files.createDirectories(logs);
        System.setProperty("cassandra.logdir", logs.toString());

        Object wrapper = newWrapper(node.isolatedLoader());
        try {
            NotificationListener inspector = (notification, handback) -> { };
            subscribeToGarbageCollectors(inspector);
            setField(wrapper, "gcInspector", inspector);

            // A platform MBeanServer that cannot be queried. Whatever the reason — a security
            // manager, a server mid-teardown — the listener is still attached afterwards.
            invoke(wrapper, "detachGcInspectorFrom", MBeanServer.class, unusableServer());

            assertThat(field(wrapper, "gcInspector"))
                    .as("the listener is still on the platform beans; dropping the only reference"
                            + " to it is what makes close()'s retry unable to do anything")
                    .isSameAs(inspector);

            // Which is what the retry is for.
            invoke(wrapper, "detachGcInspector");

            assertThat(field(wrapper, "gcInspector"))
                    .as("now it really is off, and a third call has nothing to do")
                    .isNull();
            assertThat(garbageCollectorsStillHolding(inspector))
                    .as("the retry took the listener off every garbage-collector MXBean")
                    .isEmpty();
        } finally {
            invoke(wrapper, "close");
        }
    }

    /**
     * {@code close()} must release the private {@code MBeanServer} even when emptying it throws.
     *
     * <p>{@code MBeanServerFactory} keeps every server it creates in a JVM-global list until
     * somebody releases it, and nobody comes back for this one: {@code close()} sets its own
     * {@code closed} flag first, so a second call returns immediately, {@code
     * NodeJmxServer.releaseQuietly} swallows what it throws, and {@code CassandraService.stop()}
     * runs each teardown step exactly once. An unreleased server holds this node's MBeans, and
     * through them the isolated ClassLoader, for the life of the process.
     */
    @Test
    void closeReleasesThePrivateServerEvenWhenEmptyingItThrows(@TempDir Path home) throws Exception {
        node = TestNode.create(home);
        int before = privateMBeanServers();

        Object wrapper = newWrapper(node.isolatedLoader());
        assertThat(privateMBeanServers())
                .as("the constructor is what creates the private server")
                .isEqualTo(before + 1);

        // Poison the walk close() makes over the server's contents. The stand-in delegates
        // everything else to the real server, including equals — which is what MBeanServerFactory's
        // list.remove() compares with, so releasing this proxy releases the server behind it.
        MBeanServer real = (MBeanServer) field(wrapper, "nodeServer");
        setField(wrapper, "nodeServer", serverThatCannotBeQueried(real));

        assertThatThrownBy(() -> invoke(wrapper, "close"))
                .as("the failure is still reported; it is the compensation that must not be skipped")
                .isInstanceOf(IllegalStateException.class);

        assertThat(privateMBeanServers())
                .as("MBeanServerFactory holds it, and through it the isolated ClassLoader,"
                        + " until it is released")
                .isEqualTo(before);
    }

    // --- the wrapper, from outside its ClassLoader -------------------------------------------

    /**
     * {@code NodeMBeanWrapper} implements a Cassandra interface, so the application ClassLoader
     * cannot name it — the whole point of the isolation. Everything below therefore goes through
     * the isolated loader reflectively, exactly as {@code CassandraDecommissionTest} does.
     *
     * <p>The property dance is not optional. {@code MBeanWrapper} declares default methods, so
     * initialising an implementation initialises the interface too, and its {@code instance =
     * create()} reads the JVM-global {@code org.apache.cassandra.mbean_registration_class} — which
     * every node started earlier in this JVM has set to this very class — and constructs a
     * <i>second</i> wrapper with a second private {@code MBeanServer} that nothing will ever
     * release. Cleared for the duration, so this test creates exactly one and the counts below
     * mean what they say. The isolated loader here belongs to no node, so nothing else reads it.
     */
    private static Object newWrapper(ClassLoader loader) throws Exception {
        String registration = System.getProperty(REGISTRATION_PROPERTY);
        System.clearProperty(REGISTRATION_PROPERTY);
        try {
            return Class.forName(WRAPPER_CLASS, true, loader).getDeclaredConstructor().newInstance();
        } finally {
            if (registration != null) {
                System.setProperty(REGISTRATION_PROPERTY, registration);
            }
        }
    }

    private static void invoke(Object wrapper, String method) throws Exception {
        call(wrapper.getClass().getDeclaredMethod(method), wrapper);
    }

    private static void invoke(Object wrapper, String method, Class<?> type, Object argument)
            throws Exception {
        call(wrapper.getClass().getDeclaredMethod(method, type), wrapper, argument);
    }

    private static void call(Method target, Object wrapper, Object... arguments) throws Exception {
        target.setAccessible(true);
        try {
            target.invoke(wrapper, arguments);
        } catch (InvocationTargetException e) {
            // Unwrapped, so that assertThatThrownBy sees what the method actually threw.
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw new IllegalStateException(e.getCause());
        }
    }

    private static Object field(Object wrapper, String name) throws Exception {
        return accessible(wrapper, name).get(wrapper);
    }

    private static void setField(Object wrapper, String name, Object value) throws Exception {
        accessible(wrapper, name).set(wrapper, value);
    }

    private static Field accessible(Object wrapper, String name) throws Exception {
        Field field = wrapper.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    // --- the platform beans ------------------------------------------------------------------

    private static void subscribeToGarbageCollectors(NotificationListener listener) throws Exception {
        MBeanServer platform = ManagementFactory.getPlatformMBeanServer();
        for (ObjectName name : garbageCollectors()) {
            platform.addNotificationListener(name, listener, null, null);
        }
    }

    /** @return the garbage-collector MXBeans this listener is still subscribed to */
    private static List<ObjectName> garbageCollectorsStillHolding(NotificationListener listener)
            throws Exception {
        MBeanServer platform = ManagementFactory.getPlatformMBeanServer();
        List<ObjectName> holding = new ArrayList<>();
        for (ObjectName name : garbageCollectors()) {
            try {
                // Destructive, and that is fine: this runs once, at the end, and a listener that
                // was still there has just been removed by the check that found it.
                platform.removeNotificationListener(name, listener);
                holding.add(name);
            } catch (ListenerNotFoundException expected) {
                // Not subscribed, which is the answer this test wants.
            }
        }
        return holding;
    }

    private static List<ObjectName> garbageCollectors() throws Exception {
        return new ArrayList<>(ManagementFactory.getPlatformMBeanServer().queryNames(
                new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",*"), null));
    }

    /** How many of this node's private MBean servers the JVM is still holding. */
    private static int privateMBeanServers() {
        int found = 0;
        for (MBeanServer server : MBeanServerFactory.findMBeanServer(null)) {
            // A compile-time String constant, so naming it here does not load the class.
            if (NodeMBeanWrapper.DEFAULT_DOMAIN.equals(server.getDefaultDomain())) {
                found++;
            }
        }
        return found;
    }

    // --- servers that fail --------------------------------------------------------------------

    private static MBeanServer unusableServer() {
        return proxy((proxy, method, args) -> {
            throw new IllegalStateException("this MBeanServer cannot be queried");
        });
    }

    /**
     * A server that throws only from {@code queryNames}, and that answers {@code equals} for the
     * server it stands in front of — {@code MBeanServerFactory.releaseMBeanServer} removes by
     * equality, so this is what lets the release under test reach the real server.
     */
    private static MBeanServer serverThatCannotBeQueried(MBeanServer delegate) {
        return proxy((proxy, method, args) -> {
            switch (method.getName()) {
                case "queryNames":
                    throw new IllegalStateException("this MBeanServer cannot be queried");
                case "equals":
                    return args[0] == delegate || args[0] == proxy;
                case "hashCode":
                    return System.identityHashCode(delegate);
                default:
                    return method.invoke(delegate, args);
            }
        });
    }

    private static MBeanServer proxy(InvocationHandler handler) {
        return (MBeanServer) Proxy.newProxyInstance(
                NodeMBeanWrapperTest.class.getClassLoader(), new Class<?>[]{MBeanServer.class}, handler);
    }
}
