/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Set;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.QueryExp;

import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Gives the embedded node a private {@link MBeanServer} instead of the JVM-wide platform one.
 *
 * <p>Selected through Cassandra's supported extension point
 * {@code -Dorg.apache.cassandra.mbean_registration_class}, which
 * {@code MBeanWrapper.getMBeanWrapper()} resolves via {@code FBUtilities.construct()}. It must
 * therefore be public with a public no-arg constructor, and it is instantiated by Cassandra rather
 * than by us — hence the static handle, which is how {@link CassandraService} finds it again.
 *
 * <p>Writes always land in the private server, so OpenSearch and the supervisor never see a
 * Cassandra MBean. Reads go through {@link FederatedMBeanServer}, which adds the platform MXBeans
 * that {@code nodetool} and {@code GCInspector} need.
 */
public final class NodeMBeanWrapper implements MBeanWrapper {

    /**
     * Default domain of the private server. Only used to tell servers apart in a JVM that holds
     * more than one node, which is the in-JVM multi-node test topology.
     */
    public static final String DEFAULT_DOMAIN = "cassandra-opensearch";

    /** The one MBean whose registration this class has to remember; see {@link #detachGcInspector}. */
    private static final String GC_INSPECTOR = "org.apache.cassandra.service:type=GCInspector";

    private static volatile NodeMBeanWrapper current;

    private final MBeanServer nodeServer;
    private final MBeanServer federated;

    /**
     * True once {@link #close()} has run. Every operation then becomes a no-op rather than
     * failing: Cassandra registers and unregisters metrics MBeans continuously throughout its own
     * shutdown, long after JMX has been taken down, and an exception from any of those would abort
     * the teardown sequence half way through. The fields stay non-null for the same reason.
     */
    private volatile boolean closed;

    /**
     * The {@code GCInspector} this node registered, kept because there is no other way to get at
     * it: an {@link MBeanServer} hands back attribute values, never the object it was given, and
     * {@code GCInspector.register()} keeps no reference of its own. See {@link #detachGcInspector}.
     */
    private volatile Object gcInspector;

    /**
     * Creates the private server and publishes this wrapper as {@link #current()}.
     *
     * <p>The two are not one statement apart by accident: {@code createMBeanServer} adds the server
     * to {@code MBeanServerFactory}'s JVM-global list on the first line, and {@code current = this}
     * — the only handle anything else ever gets — is assigned on the last. A throw in between would
     * leave a server nobody can name and nobody can release, holding the isolated ClassLoader for
     * the life of the process, so the window is closed here rather than described.
     */
    public NodeMBeanWrapper() {
        MBeanServer server = MBeanServerFactory.createMBeanServer(DEFAULT_DOMAIN);
        try {
            this.federated = FederatedMBeanServer.federate(server);
        } catch (Throwable t) {
            MBeanServerFactory.releaseMBeanServer(server);
            throw t;
        }
        this.nodeServer = server;
        current = this;
    }

    /** The wrapper Cassandra instantiated in this ClassLoader, or null if it never did. */
    static NodeMBeanWrapper current() {
        return current;
    }

    /** The server holding only this node's MBeans. */
    MBeanServer nodeServer() {
        return nodeServer;
    }

    @Override
    public void registerMBean(Object mbean, ObjectName name, OnException onException) {
        if (closed) {
            return;
        }
        try {
            nodeServer.registerMBean(mbean, name);
            if (GC_INSPECTOR.equals(name.getCanonicalName())) {
                gcInspector = mbean;
            }
        } catch (Exception e) {
            onException.handler.accept(e);
        }
    }

    @Override
    public void unregisterMBean(ObjectName name, OnException onException) {
        if (closed) {
            return;
        }
        try {
            nodeServer.unregisterMBean(name);
        } catch (Exception e) {
            onException.handler.accept(e);
        }
    }

    @Override
    public boolean isRegistered(ObjectName name, OnException onException) {
        if (closed) {
            return false;
        }
        try {
            return federated.isRegistered(name);
        } catch (Exception e) {
            onException.handler.accept(e);
            return false;
        }
    }

    @Override
    public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
        return closed ? Collections.emptySet() : federated.queryNames(name, query);
    }

    @Override
    public MBeanServer getMBeanServer() {
        return federated;
    }

    /**
     * Unregisters everything this node put in its private server, takes its {@code GCInspector}
     * back off the platform beans, and releases the server. Safe to call more than once; safe to
     * call before the rest of the node has been torn down.
     */
    void close() {
        if (closed) {
            return;
        }
        // Set before the work, not after: from here on Cassandra keeps registering and
        // unregistering metrics MBeans in a server that is on its way out, and those calls have to
        // become no-ops immediately. The release below is therefore in a finally — an idempotence
        // flag set before the work makes the retry its own contract offers unreachable, and
        // MBeanServerFactory holds this server, and through it the isolated ClassLoader, until
        // somebody releases it. Nobody comes back for it: NodeJmxServer.releaseQuietly swallows
        // what this throws, and CassandraService.stop() runs every step exactly once.
        closed = true;
        try {
            detachGcInspector();
            for (ObjectName name : nodeServer.queryNames(null, null)) {
                try {
                    if (!name.getCanonicalName().contains("MBeanServerDelegate")) {
                        nodeServer.unregisterMBean(name);
                    }
                } catch (Exception ignored) {
                    // A concurrent Cassandra shutdown may already have removed it.
                }
            }
        } finally {
            MBeanServerFactory.releaseMBeanServer(nodeServer);
            if (current == this) {
                current = null;
            }
        }
    }

    /**
     * Takes this node's {@code GCInspector} back off the JVM's garbage-collector MXBeans.
     *
     * <p>{@code GCInspector.register()} adds the inspector as a notification listener to the
     * <i>platform</i> {@code java.lang:type=GarbageCollector,*} beans — the private server is used
     * for the query that builds {@code gcStates}, but not for the subscription — and nothing in
     * Cassandra ever removes it, because in Cassandra's own process nothing outlives the node.
     * Here two things do:
     *
     * <ul>
     *   <li>The listener is reachable from a JVM-global object, so it pins the isolated
     *       ClassLoader for the life of the process, long after the node it belongs to is gone.
     *       That is the one thing this module exists to prevent.</li>
     *   <li>It keeps handling every collection. On an old-generation one it calls
     *       {@code LifecycleTransaction.rescheduleFailedDeletions()}, which submits to executors
     *       the teardown has already shut down, and throws {@code RejectedExecutionException}. The
     *       platform beans dispatch to their listeners in a plain loop with no per-listener
     *       {@code try}, so that exception also stops every listener added after it from ever
     *       seeing the notification — in a process that starts a second node, the live node's
     *       {@code GCInspector} silently stops working.</li>
     * </ul>
     *
     * <p>Package-private and idempotent, so that {@link CassandraService#stop()} can run it as its
     * <i>first</i> teardown step, before {@code drain()} shuts down the executor
     * {@code rescheduleFailedDeletions()} submits to. {@link #close()} calls it again for the
     * paths that never get that far — and that second call is the whole reason the reference is
     * dropped only once the listener is off. Clearing it first makes the idempotence real and the
     * retry theatre: a throw from the platform query would leave the listener attached with
     * nothing left that knows which object to remove, which is the ClassLoader pin this module
     * exists to prevent.
     */
    void detachGcInspector() {
        detachGcInspectorFrom(ManagementFactory.getPlatformMBeanServer());
    }

    /**
     * @param platform the server the garbage-collector MXBeans live in — always the platform one
     *                 in production. A parameter so that the test can hand in a server that fails,
     *                 which is the only way to exercise the retry described above.
     */
    void detachGcInspectorFrom(MBeanServer platform) {
        Object registered = gcInspector;
        if (!(registered instanceof NotificationListener listener)) {
            return;
        }
        try {
            ObjectName garbageCollectors =
                    new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",*");
            for (ObjectName name : platform.queryNames(garbageCollectors, null)) {
                try {
                    platform.removeNotificationListener(name, listener);
                } catch (ListenerNotFoundException ignored) {
                    // Registration is per GC bean and the set can change under a dynamic
                    // collector; one this inspector never subscribed to is not a problem.
                }
            }
            // Only now. Everything above has run, so there is nothing left to retry.
            gcInspector = null;
            // Logged, and named, because when this runs is the whole point: it has to be before
            // drain(), and the node's own log is where that ordering is visible after the fact.
            // Resolved on demand rather than held in a static field, as CassandraService does, so
            // that loading this class cannot initialise logback before cassandra.logdir is set.
            org.slf4j.LoggerFactory.getLogger(NodeMBeanWrapper.class)
                    .info("{} detached from the platform garbage-collector MXBeans", GC_INSPECTOR);
        } catch (Exception ignored) {
            // Best effort, like the rest of close(): a teardown must not fail on its own cleanup.
            // The reference is kept on this path on purpose — see the javadoc above.
        }
    }
}
