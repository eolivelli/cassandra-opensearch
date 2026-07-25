/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One unpacked installation, driven through {@code bin/} as an external process.
 *
 * <p>Deliberately not embedded. Running the supervisor inside the failsafe JVM would test a
 * classpath assembled by Maven rather than the one the tarball ships, would put the CQL driver
 * and the OpenSearch server in the same loader they are meant never to share, and would hide
 * every launcher-script decision — the JVM flags, {@code CASSANDRA_INCLUDE}, the pid file, the
 * management agent — which is most of what an operator actually depends on.
 *
 * <p>Instances register themselves in {@link #live} so that {@link DumpLogsOnFailure} can print
 * the server's own logs for a failed test, and so that a leaked process is a loud failure rather
 * than a port collision in whichever test happens to run next.
 */
final class DistributionNode implements AutoCloseable {

    private static final Set<DistributionNode> live = ConcurrentHashMap.newKeySet();

    /** Generous: a cold Cassandra plus a cold OpenSearch on a loaded machine. */
    private static final Duration START_TIMEOUT = Duration.ofMinutes(6);
    private static final Duration STOP_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration CLI_TIMEOUT = Duration.ofMinutes(2);

    /**
     * Small on purpose. Two nodes at the shipped 2g heap plus 2g of direct memory each would ask
     * this machine for 8 GiB to run a test that stores a few dozen rows.
     */
    private static final String HEAP = "1g";
    private static final String DIRECT_MEMORY = "1g";

    private final String name;
    private final Path home;
    private final NodeEndpoints endpoints;
    private final Map<String, String> environment;

    private volatile long pid;
    private volatile boolean killed;

    private DistributionNode(String name, Path home, NodeEndpoints endpoints) {
        this.name = name;
        this.home = home;
        this.endpoints = endpoints;
        this.environment = environment(endpoints);
    }

    /**
     * Unpacks the tarball into {@code directory} and rewrites the shipped configuration for this
     * node's endpoints and neighbours.
     *
     * <p>What is changed is only addresses, ports and cluster membership. Everything else — the
     * JVM options, the logging configuration, the crypto provider, the classpath jars — is left
     * exactly as shipped, because those are the parts the ITs are here to exercise.
     */
    static DistributionNode install(Path directory, String name, NodeEndpoints endpoints,
                                    ClusterTopology topology) {
        Path home = Distribution.unpack(directory);
        DistributionNode node = new DistributionNode(name, home, endpoints);
        node.configure(topology);
        live.add(node);
        return node;
    }

    private void configure(ClusterTopology topology) {
        // Cassandra's ports and listen address come from its own conf/cassandra.yaml, which
        // Cassandra parses itself. The identically-named keys in cassandra-opensearch.yaml reach
        // nothing — docs/KNOWN-GAPS.md §2 — so both files have to be edited, and edited to agree.
        new ConfigFile(home.resolve("conf/cassandra.yaml"))
                .set("storage_port", endpoints.storagePort())
                .set("ssl_storage_port", endpoints.sslStoragePort())
                .set("native_transport_port", endpoints.nativePort())
                .set("listen_address", endpoints.host())
                .set("rpc_address", endpoints.host())
                .setSeeds(topology.cassandraSeed())
                .save();

        ConfigFile supervisor = new ConfigFile(home.resolve("conf/cassandra-opensearch.yaml"))
                .set("listen_address", endpoints.host())
                .set("storage_port", endpoints.storagePort())
                .set("native_transport_port", endpoints.nativePort())
                .set("jmx_port", endpoints.cassandraJmxPort())
                .set("network_host", endpoints.host())
                .set("http_port", endpoints.httpPort())
                .set("transport_port", endpoints.transportPort());
        if (!"127.0.0.1".equals(endpoints.host())) {
            // Not in the shipped file because 127.0.0.1 is its default. A second node has to bind
            // its JMX connector somewhere else, or the two nodes fight over one address:port.
            supervisor.addAfter("jmx_port", "jmx_address", endpoints.host());
        }
        supervisor.save();

        if (!topology.openSearchSeedHosts().isEmpty()) {
            new ConfigFile(home.resolve("conf/opensearch.yml"))
                    .replaceLineStartingWith("discovery.type:", """
                            discovery.seed_hosts: [%s]
                            cluster.initial_cluster_manager_nodes: [%s]"""
                            .formatted(quoted(topology.openSearchSeedHosts()),
                                    quoted(topology.openSearchBootstrap())))
                    .save();
        }
    }

    private static String quoted(List<String> values) {
        return String.join(", ", values.stream().map(value -> '"' + value + '"').toList());
    }

    private static Map<String, String> environment(NodeEndpoints endpoints) {
        Map<String, String> environment = new LinkedHashMap<>();
        // The launcher requires a JDK 21 and refuses anything else. The failsafe JVM is one.
        environment.put("JAVA_HOME", System.getProperty("java.home"));
        environment.put("CASSANDRA_OPENSEARCH_JMX_HOST", endpoints.host());
        environment.put("CASSANDRA_OPENSEARCH_JMX_PORT", String.valueOf(endpoints.supervisorJmxPort()));
        environment.put("MAX_HEAP_SIZE", HEAP);
        environment.put("MAX_DIRECT_MEMORY_SIZE", DIRECT_MEMORY);
        environment.put("CASSANDRA_OPENSEARCH_STARTUP_TIMEOUT", String.valueOf(START_TIMEOUT.toSeconds()));
        environment.put("CASSANDRA_OPENSEARCH_STOP_TIMEOUT", String.valueOf(STOP_TIMEOUT.toSeconds()));
        return environment;
    }

    // --- lifecycle ---------------------------------------------------------------------------

    /** {@code bin/cassandra-opensearch start -d}, which returns only once the supervisor answers. */
    DistributionNode start() {
        Commands.Result result = run(START_TIMEOUT, "bin/cassandra-opensearch", "start", "-d");
        assertThat(result.succeeded()).as("start -d failed:%n%s", result.describe()).isTrue();
        pid = readPidFile().orElseThrow(() -> new AssertionError(
                "start -d succeeded but wrote no pid file at " + pidFile() + "\n" + result.output()));
        assertThat(ProcessHandle.of(pid)).as("pid %s from %s is not a live process", pid, pidFile())
                .isPresent();
        return this;
    }

    /** {@code bin/cassandra-opensearch stop}: SIGTERM, then wait for the ordered shutdown. */
    Commands.Result stop() {
        return run(STOP_TIMEOUT, "bin/cassandra-opensearch", "stop");
    }

    Commands.Result cli(String... arguments) {
        List<String> command = new ArrayList<>(List.of("bin/cassandra-opensearch"));
        command.addAll(List.of(arguments));
        return run(CLI_TIMEOUT, command.toArray(new String[0]));
    }

    /**
     * {@code bin/nodetool}, pointed at this node by nothing but its configuration.
     *
     * <p>Neither {@code -h} nor {@code -p} is passed, deliberately: the wrapper reads
     * {@code jmx_address} and {@code jmx_port} out of {@code conf/cassandra-opensearch.yaml},
     * and that derivation is the thing worth testing — an operator on a node whose JMX is not on
     * 127.0.0.1 has nothing else to go on. Supplying the host here would exercise the operator's
     * override and leave the default path, which is the one everybody actually uses, untested.
     */
    Commands.Result nodetool(String... arguments) {
        List<String> command = new ArrayList<>(List.of("bin/nodetool"));
        command.addAll(List.of(arguments));
        return run(CLI_TIMEOUT, command.toArray(new String[0]));
    }

    Commands.Result run(Duration timeout, String... command) {
        List<String> resolved = new ArrayList<>();
        resolved.add(home.resolve(command[0]).toString());
        resolved.addAll(List.of(command).subList(1, command.length));
        return Commands.run(home, resolved, environment, timeout);
    }

    /**
     * Kills the process if anything is still running, and records that it had to.
     *
     * <p>This is the last line of defence, not the shutdown path: a node that is still up here
     * has failed the very thing the lifecycle IT asserts, and leaving it holding its ports would
     * turn one failure into a failure of every test that runs afterwards.
     */
    @Override
    public void close() {
        live.remove(this);
        Optional<ProcessHandle> process = ProcessHandle.of(pid).filter(ProcessHandle::isAlive);
        if (process.isEmpty()) {
            return;
        }
        killed = true;
        dumpLogs("node '" + name + "' was still running at teardown");
        ProcessHandle handle = process.get();
        handle.destroy();
        try {
            handle.onExit().get(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            handle.destroyForcibly();
        }
    }

    /** True when {@link #close()} had to intervene — the process did not exit on its own. */
    boolean wasKilled() {
        return killed;
    }

    // --- observation -------------------------------------------------------------------------

    String name() {
        return name;
    }

    Path home() {
        return home;
    }

    NodeEndpoints endpoints() {
        return endpoints;
    }

    long pid() {
        return pid;
    }

    Path pidFile() {
        return home.resolve("cassandra-opensearch.pid");
    }

    Optional<Long> readPidFile() {
        try {
            return Files.exists(pidFile())
                    ? Optional.of(Long.parseLong(Files.readString(pidFile()).trim()))
                    : Optional.empty();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    boolean isAlive() {
        return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    }

    Rest rest() {
        return new Rest(endpoints.host(), endpoints.httpPort());
    }

    /**
     * Everything the supervisor itself wrote, which is where the phase-by-phase decommission
     * record and the ordered shutdown live. Cassandra's and OpenSearch's own logs are separate
     * files, written by logback and log4j2 from inside their loaders.
     */
    String supervisorLog() {
        return read(home.resolve("logs/cassandra-opensearch.log"));
    }

    void awaitExit(Duration timeout) {
        Await.until("the process (pid " + pid + ") exits", timeout,
                () -> "still alive: " + isAlive(), () -> !isAlive());
    }

    // --- diagnostics -------------------------------------------------------------------------

    static void dumpAllLive(String reason) {
        live.forEach(node -> node.dumpLogs(reason));
    }

    /**
     * Prints this node's logs to the build output.
     *
     * <p>An IT that fails with "connection refused" and nothing else is close to useless: the
     * reason is always in the server's log, which lives in a temporary directory the reader of a
     * CI log cannot reach.
     */
    void dumpLogs(String reason) {
        StringBuilder report = new StringBuilder();
        report.append("\n==== logs for node '").append(name).append("' (").append(reason).append(") ====\n")
                .append("home: ").append(home).append("  pid: ").append(pid)
                .append("  alive: ").append(isAlive()).append('\n');
        Path logs = home.resolve("logs");
        if (!Files.isDirectory(logs)) {
            report.append("no logs directory at ").append(logs).append('\n');
            System.out.println(report);
            return;
        }
        try (Stream<Path> files = Files.list(logs)) {
            files.filter(Files::isRegularFile)
                    .filter(file -> file.getFileName().toString().endsWith(".log"))
                    .sorted()
                    .forEach(file -> report.append(tail(file, 200)));
        } catch (IOException e) {
            report.append("cannot list ").append(logs).append(": ").append(e).append('\n');
        }
        System.out.println(report);
    }

    private static String tail(Path file, int lines) {
        String content = read(file);
        List<String> all = List.of(content.split("\n", -1));
        List<String> last = all.subList(Math.max(0, all.size() - lines), all.size());
        return "\n---- " + file.getFileName() + " (last " + last.size() + " of " + all.size()
                + " lines) ----\n" + String.join("\n", last) + '\n';
    }

    private static String read(Path file) {
        try {
            return Files.isRegularFile(file) ? Files.readString(file) : "";
        } catch (IOException e) {
            return "cannot read " + file + ": " + e;
        }
    }
}
