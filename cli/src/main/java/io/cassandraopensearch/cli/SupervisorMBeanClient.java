/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.cli;

import java.io.IOException;
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
 *   <li>an operation {@code stop()} or {@code shutdown()};</li>
 *   <li>an operation {@code decommission(long timeoutMillis, boolean force)}, or one of the
 *       simpler shapes listed in {@link #DECOMMISSION_SIGNATURES}.</li>
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
            connector = JMXConnectorFactory.connect(new JMXServiceURL(serviceUrl), null);
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
                .orElseThrow(() -> new CliException(CliException.FAILED,
                        "No no-argument stop operation on " + supervisor + "; it declares "
                                + operationSummary()));
        invoke(operation.getName(), new Object[0], new String[0]);
    }

    /**
     * @param timeoutMillis bound on each waiting phase of the decommission
     * @param force         proceed even with shards or ranges still outstanding
     * @return the name of the operation that was invoked, for the operator's benefit — the
     *         supervisor may not support every argument that was asked for
     */
    String decommission(long timeoutMillis, boolean force) {
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
                    arguments[i] = "boolean".equals(actual[i]) ? Boolean.valueOf(force) : Long.valueOf(timeoutMillis);
                }
                invoke(candidate.getName(), arguments, actual);
                return describe(candidate);
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
