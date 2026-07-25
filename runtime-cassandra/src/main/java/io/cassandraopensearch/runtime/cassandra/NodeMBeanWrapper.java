/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.util.Collections;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
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

    public NodeMBeanWrapper() {
        this.nodeServer = MBeanServerFactory.createMBeanServer(DEFAULT_DOMAIN);
        this.federated = FederatedMBeanServer.federate(nodeServer);
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
     * Unregisters everything this node put in its private server and releases the server. Safe to
     * call more than once; safe to call before the rest of the node has been torn down.
     */
    void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (ObjectName name : nodeServer.queryNames(null, null)) {
            try {
                if (!name.getCanonicalName().contains("MBeanServerDelegate")) {
                    nodeServer.unregisterMBean(name);
                }
            } catch (Exception ignored) {
                // A concurrent Cassandra shutdown may already have removed it.
            }
        }
        MBeanServerFactory.releaseMBeanServer(nodeServer);
        if (current == this) {
            current = null;
        }
    }
}
