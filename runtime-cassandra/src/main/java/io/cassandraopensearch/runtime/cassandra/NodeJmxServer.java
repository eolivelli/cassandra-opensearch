/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.net.InetAddress;
import java.rmi.Remote;
import java.rmi.server.RMIServerSocketFactory;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.management.remote.rmi.RMIJRMPServerImpl;

import org.apache.cassandra.utils.JMXServerUtils;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.RMIServerSocketFactoryImpl;

/**
 * The RMI connector that lets the stock {@code bin/nodetool} manage this embedded node.
 *
 * <p>Cassandra's own {@code CassandraDaemon.maybeInitJmx()} is deliberately left inert — we never
 * set {@code cassandra.jmx.local.port} or {@code cassandra.jmx.remote.port} — because the connector
 * has to be built around the node's private MBeanServer rather than the platform one.
 *
 * <p>Three details separate this from Cassandra's in-JVM test equivalent, {@code IsolatedJmx}, and
 * all three matter for an out-of-process client:
 *
 * <ol>
 *   <li><b>No RMI client socket factory.</b> {@code IsolatedJmx} supplies its own; that object is
 *       serialised into the stub and shipped to the client, where the class does not exist, so a
 *       remote {@code nodetool} fails with {@code ClassNotFoundException}. Passing null gets the
 *       JDK default.</li>
 *   <li><b>{@code java.rmi.server.hostname} is pinned</b> before export, or the stub advertises a
 *       non-loopback address and the client dials the wrong interface. Note this is read once per
 *       JVM and cached by RMI, so several nodes in one process must differ by port, not address.</li>
 *   <li><b>The connector is given the federated server</b>, not the private one — see
 *       {@link FederatedMBeanServer}.</li>
 * </ol>
 *
 * <p>Must be started <i>before</i> {@code CassandraDaemon.activate()}: constructing this class is
 * what forces {@code MBeanWrapper.instance} to resolve to {@link NodeMBeanWrapper}, and
 * {@code GCInspector} reads that during {@code setup()}.
 */
final class NodeJmxServer {

    private final JMXConnectorServer connectorServer;
    private final JMXServerUtils.JmxRegistry registry;
    private final NodeMBeanWrapper wrapper;
    private final String serviceUrl;

    private NodeJmxServer(JMXConnectorServer connectorServer,
                          JMXServerUtils.JmxRegistry registry,
                          NodeMBeanWrapper wrapper,
                          String serviceUrl) {
        this.connectorServer = connectorServer;
        this.registry = registry;
        this.wrapper = wrapper;
        this.serviceUrl = serviceUrl;
    }

    static NodeJmxServer start(String bindAddress, int port) throws Exception {
        System.setProperty("java.rmi.server.hostname", bindAddress);
        System.setProperty("java.rmi.server.randomIDs", "true");
        // Keep RMI's distributed-GC and reaper threads short-lived; they would otherwise hold the
        // isolated ClassLoader alive for ten minutes after the node has stopped.
        System.setProperty("java.rmi.dgc.leaseValue", "1000");
        System.setProperty("sun.rmi.transport.tcp.threadKeepAliveTime", "1000");

        // Touching MBeanWrapper.instance runs FBUtilities.construct() on the class named by
        // org.apache.cassandra.mbean_registration_class, which is how our wrapper gets created.
        MBeanServer federated = MBeanWrapper.instance.getMBeanServer();
        NodeMBeanWrapper wrapper = NodeMBeanWrapper.current();
        if (wrapper == null) {
            // Outside the compensation below on purpose: no NodeMBeanWrapper was constructed, so no
            // private MBeanServer was created either — whatever MBeanWrapper.instance resolved to
            // is using the platform server, which is not ours to release.
            throw new IllegalStateException(
                    "MBeanWrapper.instance resolved to " + MBeanWrapper.instance.getClass().getName()
                            + " instead of " + NodeMBeanWrapper.class.getName()
                            + ". The system property org.apache.cassandra.mbean_registration_class must be"
                            + " set before any Cassandra class touches MBeanWrapper.");
        }

        // Everything from here on can fail, and by this point the private MBeanServer exists.
        // MBeanServerFactory holds every server it creates in a JVM-global list until it is
        // explicitly released, so a server dropped on the floor here keeps this node's MBeans — and
        // the isolated ClassLoader that loaded them — alive for the life of the process. start() is
        // called once, from a start() that will not be retried, so nothing else ever comes back for
        // it: CassandraService.stop() takes the daemon == null / jmx == null branch and never sees
        // this wrapper.
        //
        // The try opens HERE, above the socket factory, and not at the registry below. Both
        // statements in between throw on ordinary misconfiguration —
        // InetAddress.getByName(bindAddress) with an unresolvable cassandra.jmx.address is an
        // UnknownHostException before a single socket has been asked for — and a boundary drawn
        // after them leaks exactly the same MBeanServer as no boundary at all.
        JMXServerUtils.JmxRegistry registry = null;
        JMXConnectorServer connectorServer = null;
        try {
            RMIServerSocketFactory socketFactory =
                    new RMIServerSocketFactoryImpl(InetAddress.getByName(bindAddress));
            Map<String, Object> env = new HashMap<>();
            env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, socketFactory);
            env.put("jmx.remote.x.daemon", "true");
            env.put("jmx.remote.rmi.server.credentials.filter.pattern", String.class.getName() + ";!*");

            registry = new JMXServerUtils.JmxRegistry(port, null, socketFactory, "jmxrmi");
            RMIJRMPServerImpl rmiServer = new RMIJRMPServerImpl(port, null, socketFactory, env);
            JMXServiceURL url = new JMXServiceURL("rmi", bindAddress, port);
            connectorServer = new RMIConnectorServer(url, env, rmiServer, federated);
            connectorServer.start();
            registry.setRemoteServerStub((Remote) rmiServer.toStub());

            String serviceUrl = "service:jmx:rmi://" + bindAddress + ':' + port
                    + "/jndi/rmi://" + bindAddress + ':' + port + "/jmxrmi";
            return new NodeJmxServer(connectorServer, registry, wrapper, serviceUrl);
        } catch (Throwable failure) {
            releaseQuietly(connectorServer, registry, wrapper);
            throw failure;
        }
    }

    /** Gives back everything {@link #start} had taken by the time it failed. */
    private static void releaseQuietly(JMXConnectorServer connectorServer,
                                       JMXServerUtils.JmxRegistry registry,
                                       NodeMBeanWrapper wrapper) {
        if (connectorServer != null) {
            try {
                connectorServer.stop();
            } catch (Exception ignored) {
                // Never started, or already down; the original failure is the one worth reporting.
            }
        }
        if (registry != null) {
            try {
                registry.close();
            } catch (Exception ignored) {
                // Same.
            }
        }
        // Last, and unconditionally: this is the one that releases the MBeanServer.
        try {
            wrapper.close();
        } catch (Exception ignored) {
            // Same.
        }
    }

    /** The URL a remote {@code nodetool} or {@code JMXConnector} connects to. */
    String serviceUrl() {
        return serviceUrl;
    }

    /**
     * Takes this node's {@code GCInspector} off the platform garbage-collector MXBeans, and nothing
     * else.
     *
     * <p>Separate from {@link #stop()} because it has to happen earlier than {@code stop()} does.
     * {@code stop()} is the third step of {@link CassandraService#stop()}, after
     * {@code destroyClientTransports()} and {@code drain()} — and {@code drain()} shuts down the
     * executor that {@code GCInspector}'s notification handler submits
     * {@code LifecycleTransaction.rescheduleFailedDeletions()} to. See
     * {@link NodeMBeanWrapper#detachGcInspector()} for what a throw from there costs.
     */
    void detachGcInspector() {
        wrapper.detachGcInspector();
    }

    /**
     * Stops the connector and empties the private MBeanServer. Called early in the teardown, well
     * before the executors go down, which is safe only because {@link NodeMBeanWrapper} turns into
     * a no-op rather than starting to throw.
     */
    void stop() {
        try {
            connectorServer.stop();
        } catch (Exception ignored) {
            // Already stopped, or a client disconnected mid-call; neither blocks the teardown.
        }
        registry.close();
        wrapper.close();
    }
}
