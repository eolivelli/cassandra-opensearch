/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.cli;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.management.Attribute;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * The CLI's half of the supervisor's JMX MBean.
 *
 * <p>The MBean lives in the {@code io.cassandraopensearch} domain on the platform MBean server,
 * which the launcher exposes through the JDK's own management agent — deliberately a different
 * connector from the one {@code nodetool} uses. That one belongs to the embedded Cassandra node
 * and serves a federated MBean server whose non-platform half is private to the child
 * ClassLoader, so the supervisor's MBean is not reachable through it.
 *
 * <h2>Why this is reflective</h2>
 *
 * The CLI cannot depend on the {@code server} module: {@code lib/boot} is the JVM's {@code -cp}
 * and every jar on it is visible to both isolated loaders, so the boot classpath is kept to the
 * minimum — and a management client that shares a compiled interface with the process it manages
 * is also a client that breaks the moment the two are a version apart. So this looks the
 * operations up in the {@link MBeanInfo} at runtime and picks the first signature it recognises.
 * The contract it expects is:
 *
 * <ul>
 *   <li>an MBean in domain {@code io.cassandraopensearch}, by preference
 *       {@code io.cassandraopensearch:type=Supervisor};</li>
 *   <li>readable attributes describing the process — printed as-is by {@code status};</li>
 *   <li>an operation {@code decommission(long timeoutSeconds, boolean force)}, or one of the
 *       simpler shapes listed in {@link #DECOMMISSION_SIGNATURES};</li>
 *   <li>optionally {@code Map serviceDetails(String)} and a {@code DecommissionProgress}
 *       attribute, both of which the CLI renders when they are there and skips when they are not;</li>
 *   <li>optionally an operation {@code stop()} or {@code shutdown()}. There is none today —
 *       the supervisor owns the JVM's only shutdown hook, so a signal is the stop mechanism and
 *       the launcher falls back to one. The lookup stays because a stop operation is the more
 *       precise instrument if one is ever added.</li>
 * </ul>
 *
 * <p>When an expectation is not met the failure names the MBean and lists what it did find,
 * because "the operation is not there" is a fact an operator can act on and a stack trace is not.
 */
final class SupervisorMBeanClient implements AutoCloseable {

    static final String DOMAIN = "io.cassandraopensearch";
    private static final String PREFERRED_NAME = DOMAIN + ":type=Supervisor";

    /** Accepted {@code stop} shapes, in order of preference. */
    private static final List<String> STOP_OPERATIONS = List.of("stop", "shutdown", "stopNode");

    /**
     * Accepted {@code decommission} shapes, most expressive first. The parameter types are what
     * the operation must declare; anything else with the same name is reported rather than
     * guessed at.
     */
    private static final List<String[]> DECOMMISSION_SIGNATURES = List.of(
            new String[] {"long", "boolean"},
            new String[] {"boolean", "long"},
            new String[] {"boolean"},
            new String[] {"long"},
            new String[] {});

    /**
     * How long any single network operation this client performs may take.
     *
     * <p>Ten seconds and not the JDK default, which is no bound at all. {@code
     * JMXConnectorFactory.connect} with a null environment hands the TCP connect to the operating
     * system, so a JMX host that drops packets rather than refusing them — a firewall, a
     * disappeared container, a stale address in {@code CASSANDRA_OPENSEARCH_JMX_HOST} — blocks for
     * the kernel's SYN retry budget, measured at over 25 seconds on Linux and considerably longer
     * elsewhere. That is not merely a slow {@code status}: {@code bin/cassandra-opensearch}'s
     * {@code refuse_if_running} runs this on the way into <i>every</i> start, including the
     * container image's {@code CMD}, so an unreachable JMX address delays every start by it.
     */
    private static final int TIMEOUT_MILLIS = Integer.getInteger(
            "cassandra-opensearch.cli.jmx.timeout.millis", 10_000);

    static {
        // Bounds how long an idle pooled connection is kept before being discarded. The RMI
        // transport reads it once, from a static initialiser of a class this process only ever
        // loads through the connect below, so setting it here is early enough. Not overwritten:
        // an operator who set it on the command line meant it.
        defaultProperty("sun.rmi.transport.connectionTimeout", TIMEOUT_MILLIS);

        // sun.rmi.transport.tcp.responseTimeout is deliberately NOT set, though it looks like it
        // belongs beside the line above and was set here at first. It bounds how long a client
        // waits for the *reply to a call*, not how long it waits to connect — and `decommission`
        // legitimately blocks for as long as it takes to relocate every shard and stream every
        // range, which the shipped configuration allows an hour for. With it set to ten seconds
        // the reply never arrived: the transport tore the connection down mid-call, the CLI read
        // that as the connection loss a completed decommission produces, and the operator was
        // told the node had gone while it was still streaming.
        //
        // The connect this class actually needed to bound is the first hop, and BoundedSocketFactory
        // below does that without touching call durations.
    }

    private static void defaultProperty(String name, int millis) {
        if (System.getProperty(name) == null) {
            System.setProperty(name, Integer.toString(millis));
        }
    }

    private final JMXConnector connector;
    private final MBeanServerConnection connection;
    private final ObjectName supervisor;
    private final String serviceUrl;

    private SupervisorMBeanClient(JMXConnector connector, MBeanServerConnection connection,
                                  ObjectName supervisor, String serviceUrl) {
        this.connector = connector;
        this.connection = connection;
        this.supervisor = supervisor;
        this.serviceUrl = serviceUrl;
    }

    /**
     * @param serviceUrl a full {@code service:jmx:rmi://...} URL
     * @throws CliException if the process is not reachable, or is reachable but registers no
     *                      MBean in the supervisor's domain
     */
    static SupervisorMBeanClient connect(String serviceUrl) {
        JMXConnector connector;
        try {
            connector = JMXConnectorFactory.connect(new JMXServiceURL(serviceUrl), connectEnvironment());
        } catch (IOException e) {
            throw new CliException(CliException.NOT_RUNNING,
                    "Cannot reach a cassandra-opensearch process at " + serviceUrl
                            + " (" + rootCause(e) + ")."
                            + " Either it is not running, or it was started with a different"
                            + " CASSANDRA_OPENSEARCH_JMX_PORT; see bin/cassandra-opensearch.in.sh.");
        }
        try {
            MBeanServerConnection connection = connector.getMBeanServerConnection();
            return new SupervisorMBeanClient(connector, connection, findSupervisor(connection), serviceUrl);
        } catch (RuntimeException | IOException e) {
            closeQuietly(connector);
            if (e instanceof CliException cliException) {
                throw cliException;
            }
            throw new CliException(CliException.FAILED, "JMX connection to " + serviceUrl + " failed", e);
        }
    }

    /**
     * The environment every connection this class makes is built with.
     *
     * <p>Package-private so the timeouts can be asserted: an unbounded connect is invisible until
     * the day it is not, and "it was fast on my machine" is not evidence that a bound exists.
     *
     * <ul>
     *   <li>{@code com.sun.jndi.rmi.factory.socket} is the one that matters. The first thing a
     *       {@code service:jmx:rmi:///jndi/rmi://host:port/jmxrmi} URL does is a JNDI lookup of the
     *       RMI registry, and that is the hop that hangs against an unreachable host. It is also
     *       the only socket in the exchange this side gets to choose.</li>
     *   <li>{@code jmx.remote.x.client.connection.check.period} switches off the client's
     *       background heartbeat. This CLI is a short-lived process that makes one call and exits;
     *       the heartbeat only adds a non-daemon thread and a second way to block.</li>
     * </ul>
     */
    static Map<String, Object> connectEnvironment() {
        Map<String, Object> environment = new LinkedHashMap<>();
        environment.put("com.sun.jndi.rmi.factory.socket", new BoundedSocketFactory(TIMEOUT_MILLIS));
        environment.put("jmx.remote.x.client.connection.check.period", 0L);
        return environment;
    }

    /**
     * An {@link RMIClientSocketFactory} that connects with a deadline instead of without one.
     *
     * <p>{@code new Socket(host, port)} blocks in the kernel until the SYN retry budget runs out;
     * {@code Socket.connect(address, timeout)} does not. The read timeout is set for the same
     * reason on the other side of the exchange: a host that accepts the connection and then says
     * nothing is as good as a hang.
     *
     * <p><b>Scope.</b> This factory is supplied through {@code com.sun.jndi.rmi.factory.socket},
     * so it covers exactly one hop: the JNDI lookup of the {@code RMIServer} stub in the registry.
     * That is a single short request and response, which is why a read timeout is safe here. Every
     * later hop — {@code newClient()} and every MBean call after it — goes through a socket factory
     * the *server* supplies, which this class cannot reach and must not try to bound: those calls
     * include {@code decommission}, which blocks for as long as the operation takes.
     *
     * <p>{@link Serializable} because {@code RMIClientSocketFactory} implementations normally
     * travel inside a stub. This one never does — it is supplied client-side, through the JNDI
     * environment — but a factory that could not be serialised if it ever were is a trap for
     * whoever moves it.
     */
    private static final class BoundedSocketFactory implements RMIClientSocketFactory, Serializable {

        private static final long serialVersionUID = 1L;

        private final int timeoutMillis;

        BoundedSocketFactory(int timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            Socket socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(host, port), timeoutMillis);
                socket.setSoTimeout(timeoutMillis);
            } catch (IOException | RuntimeException failure) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Already failing for a better reason.
                }
                throw failure;
            }
            return socket;
        }

        @Override
        public boolean equals(Object other) {
            // RMI compares factories to decide whether two stubs may share a connection; without
            // this every lookup would open its own.
            return other instanceof BoundedSocketFactory factory
                    && factory.timeoutMillis == timeoutMillis;
        }

        @Override
        public int hashCode() {
            return timeoutMillis;
        }

        @Override
        public String toString() {
            return "BoundedSocketFactory(" + timeoutMillis + "ms)";
        }
    }

    private static ObjectName findSupervisor(MBeanServerConnection connection) throws IOException {
        Set<ObjectName> names;
        try {
            names = connection.queryNames(new ObjectName(DOMAIN + ":*"), null);
        } catch (javax.management.MalformedObjectNameException e) {
            throw new IllegalStateException(e);
        }
        if (names.isEmpty()) {
            throw new CliException(CliException.FAILED,
                    "Connected, but the process registers no MBean in the " + DOMAIN + " domain."
                            + " Either it is still starting up, or it is not a cassandra-opensearch"
                            + " process at all.");
        }
        return names.stream()
                .filter(name -> PREFERRED_NAME.equals(name.getCanonicalName()))
                .findFirst()
                // Deterministic rather than whatever the query returned first: an operator
                // running `status` twice must not get two different answers.
                .orElseGet(() -> new TreeSet<>(names).first());
    }

    String serviceUrl() {
        return serviceUrl;
    }

    ObjectName objectName() {
        return supervisor;
    }

    /** Every readable attribute of the supervisor MBean, in declaration order. */
    Map<String, Object> attributes() {
        Map<String, Object> values = new LinkedHashMap<>();
        try {
            MBeanInfo info = connection.getMBeanInfo(supervisor);
            List<String> readable = new ArrayList<>();
            for (MBeanAttributeInfo attribute : info.getAttributes()) {
                if (attribute.isReadable()) {
                    readable.add(attribute.getName());
                }
            }
            for (Attribute attribute : connection.getAttributes(supervisor,
                    readable.toArray(new String[0])).asList()) {
                values.put(attribute.getName(), attribute.getValue());
            }
            // A getAttributes() bulk read silently drops attributes whose getter threw, which is
            // how a half-started service disappears from `status` output. Name them instead.
            for (String name : readable) {
                values.putIfAbsent(name, "<unavailable>");
            }
        } catch (Exception e) {
            throw new CliException(CliException.FAILED, "Cannot read " + supervisor, e);
        }
        return values;
    }

    void stop() {
        MBeanOperationInfo operation = operations().stream()
                .filter(candidate -> STOP_OPERATIONS.contains(candidate.getName()))
                .filter(candidate -> candidate.getSignature().length == 0)
                .findFirst()
                .orElseThrow(() -> new CliException(CliException.NO_SUCH_OPERATION,
                        "the supervisor MBean exposes no stop operation"));
        invoke(operation.getName(), new Object[0], new String[0]);
    }

    /** The last thing a running decommission said about itself, or null if it says nothing. */
    String decommissionProgress() {
        try {
            Object value = connection.getAttribute(supervisor, "DecommissionProgress");
            return value == null ? null : String.valueOf(value);
        } catch (Exception e) {
            return null;
        }
    }

    /** Whatever {@code serviceDetails(name)} returns, or an empty map if there is no such operation. */
    Map<String, String> serviceDetails(String service) {
        boolean supported = operations().stream()
                .anyMatch(candidate -> "serviceDetails".equals(candidate.getName())
                        && Arrays.equals(new String[] {"java.lang.String"}, parameterTypes(candidate)));
        if (!supported) {
            return Map.of();
        }
        Object result = invoke("serviceDetails", new Object[] {service}, new String[] {"java.lang.String"});
        Map<String, String> details = new LinkedHashMap<>();
        if (result instanceof Map<?, ?> map) {
            map.forEach((key, value) -> details.put(String.valueOf(key), String.valueOf(value)));
        }
        return details;
    }

    /**
     * @param timeoutSeconds bound on each waiting phase of the decommission; {@code 0} means use
     *                       the limits from {@code conf/cassandra-opensearch.yaml}
     * @param force          proceed even with shards or ranges still outstanding
     * @return whatever the operation returned, prefixed by the signature that was called — the
     *         supervisor may not accept every argument that was asked for
     */
    String decommission(long timeoutSeconds, boolean force) {
        List<MBeanOperationInfo> candidates = operations().stream()
                .filter(candidate -> "decommission".equals(candidate.getName()))
                .toList();
        if (candidates.isEmpty()) {
            throw new CliException(CliException.FAILED,
                    "No decommission operation on " + supervisor + "; it declares " + operationSummary());
        }
        for (String[] wanted : DECOMMISSION_SIGNATURES) {
            for (MBeanOperationInfo candidate : candidates) {
                String[] actual = parameterTypes(candidate);
                if (!Arrays.equals(wanted, actual)) {
                    continue;
                }
                Object[] arguments = new Object[actual.length];
                for (int i = 0; i < actual.length; i++) {
                    arguments[i] = "boolean".equals(actual[i]) ? Boolean.valueOf(force) : Long.valueOf(timeoutSeconds);
                }
                Object result = invoke(candidate.getName(), arguments, actual);
                return describe(candidate) + (result == null ? "" : ": " + result);
            }
        }
        throw new CliException(CliException.FAILED,
                "The decommission operation on " + supervisor + " has a signature this CLI does not"
                        + " know how to call: " + candidates.stream()
                        .map(SupervisorMBeanClient::describe).toList());
    }

    private Object invoke(String operation, Object[] arguments, String[] signature) {
        try {
            return connection.invoke(supervisor, operation, arguments, signature);
        } catch (Exception e) {
            throw new CliException(CliException.FAILED,
                    operation + " failed on " + supervisor + ": " + rootCause(e), e);
        }
    }

    private List<MBeanOperationInfo> operations() {
        try {
            return List.of(connection.getMBeanInfo(supervisor).getOperations());
        } catch (Exception e) {
            throw new CliException(CliException.FAILED, "Cannot introspect " + supervisor, e);
        }
    }

    private String operationSummary() {
        return operations().stream().map(SupervisorMBeanClient::describe).toList().toString();
    }

    private static String[] parameterTypes(MBeanOperationInfo operation) {
        MBeanParameterInfo[] parameters = operation.getSignature();
        String[] types = new String[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            types[i] = parameters[i].getType();
        }
        return types;
    }

    private static String describe(MBeanOperationInfo operation) {
        return operation.getName() + '(' + String.join(", ", parameterTypes(operation)) + ')';
    }

    static String rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return message == null || message.isBlank() ? cause.toString() : message;
    }

    private static void closeQuietly(JMXConnector connector) {
        try {
            connector.close();
        } catch (IOException ignored) {
            // Already failing for a better reason.
        }
    }

    @Override
    public void close() {
        closeQuietly(connector);
    }
}
