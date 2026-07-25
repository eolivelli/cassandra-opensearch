/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import io.cassandraopensearch.spi.DecommissionContext;
import io.cassandraopensearch.spi.EmbeddedService;
import io.cassandraopensearch.spi.ServiceContext;
import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.env.Environment;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.node.InternalSettingsPreparer;
import org.opensearch.node.Node;
import org.opensearch.transport.Netty4ModulePlugin;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A full OpenSearch node running inside the supervisor's JVM, in the {@code opensearch}
 * isolated ClassLoader.
 *
 * <h2>What this deliberately does not do</h2>
 *
 * Everything here goes through {@link Node} directly and never through {@code
 * org.opensearch.bootstrap.Bootstrap}, which is what makes co-hosting with Cassandra safe.
 * {@code Bootstrap} owns all the process-wide behaviour we must not inherit: it installs the
 * java agent and its JVM-wide policy, registers shutdown hooks, sets a default uncaught
 * exception handler that calls {@link System#exit}, loads JNA natives, and runs
 * {@code LogConfigurator}, which hijacks {@code java.util.logging} for the whole JVM and would
 * drag Cassandra's logging into OpenSearch's log4j2 configuration. None of it fires when the
 * node is constructed by hand.
 *
 * <p>For the same reason no {@code io.netty.*} system property is set here even though
 * OpenSearch's own launcher sets several: those are read by Cassandra's Netty out of the same
 * JVM, and {@code io.netty.noUnsafe=true} in particular measurably slows Cassandra's native
 * transport.
 *
 * <h2>Threading</h2>
 *
 * The supervisor drives the lifecycle from one thread; {@link #status()} and {@link #details()}
 * may be called concurrently from any thread and are lock-free reads. The thread context
 * ClassLoader is already pinned to the isolated loader by the caller
 * ({@code IsolatedService}), which matters more than it looks: OpenSearch resolves
 * {@code XContentBuilderExtension} through {@code ServiceLoader.load(Class)}, and a wrong TCCL
 * during {@code Node} construction permanently breaks XContent serialisation of
 * {@code TimeValue} and friends — the node starts and searches fine while
 * {@code _cluster/health}, {@code _nodes/stats} and {@code _cluster/stats} return HTTP 400.
 */
public final class OpenSearchService implements EmbeddedService {

    /** Service name; also the {@code lib/opensearch} directory and the SPI service key. */
    public static final String NAME = "opensearch";

    /**
     * Supervisor settings, from {@code conf/cassandra-opensearch.yaml}. These are layered on
     * top of {@code conf/opensearch.yml} because the supervisor owns port allocation and node
     * identity for the whole process — an operator editing {@code opensearch.yml} must not be
     * able to move OpenSearch onto a port the supervisor handed to Cassandra.
     */
    static final String SETTING_HTTP_PORT = "http_port";
    static final String SETTING_TRANSPORT_PORT = "transport_port";
    static final String SETTING_NETWORK_HOST = "network_host";
    static final String SETTING_CLUSTER_NAME = "cluster_name";
    static final String SETTING_NODE_NAME = "node_name";
    static final String SETTING_START_TIMEOUT = "start_timeout_seconds";

    private static final Duration DEFAULT_START_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration DECOMMISSION_POLL_INTERVAL = Duration.ofSeconds(1);
    private static final long CLOSE_TIMEOUT_SECONDS = 30;

    /** Cluster-wide allocation filter; setting it to our node name drains this node's shards. */
    private static final String ALLOCATION_EXCLUDE_NAME = "cluster.routing.allocation.exclude._name";

    private final AtomicReference<ServiceStatus> status = new AtomicReference<>(ServiceStatus.NEW);

    private volatile ServiceContext context;
    private volatile Node node;
    private volatile Client client;
    private volatile ClusterService clusterService;
    private volatile String nodeName;
    private volatile String nodeId;
    private volatile String httpAddress;
    private volatile String transportAddress;

    /** Memoised derivation of the applied cluster state; see {@link ClusterSnapshot}. */
    private volatile ClusterSnapshot snapshot;

    @Override
    public String name() {
        return NAME;
    }

    // --- lifecycle -------------------------------------------------------------------------

    @Override
    public void start(ServiceContext serviceContext) throws Exception {
        ServiceStatus previous = status.get();
        if (previous != ServiceStatus.NEW && previous != ServiceStatus.STOPPED) {
            throw new ServiceException(NAME, "cannot start from state " + previous);
        }
        status.set(ServiceStatus.STARTING);
        this.context = serviceContext;
        try {
            Environment environment = InternalSettingsPreparer.prepareEnvironment(
                    buildSettings(serviceContext),
                    Map.of(),
                    serviceContext.configDirectory(),
                    OpenSearchService::defaultNodeName);
            this.nodeName = Node.NODE_NAME_SETTING.get(environment.settings());

            Node started = new EmbeddedNode(environment, List.of(EmbeddedNode.classpathPlugin(Netty4ModulePlugin.class)));
            this.node = started;
            started.start();

            this.client = started.client();
            this.clusterService = started.injector().getInstance(ClusterService.class);
            this.nodeId = clusterService.localNode().getId();
            this.httpAddress = started.injector().getInstance(HttpServerTransport.class)
                    .boundAddress().publishAddress().toString();
            this.transportAddress = started.injector().getInstance(TransportService.class)
                    .boundAddress().publishAddress().toString();

            awaitHealthy(startTimeout(serviceContext));
            status.set(ServiceStatus.RUNNING);
            serviceContext.reportEvent("INFO", "opensearch.started",
                    "node " + nodeName + " (" + nodeId + ") listening on http " + httpAddress
                            + ", transport " + transportAddress);
        } catch (Throwable failure) {
            status.set(ServiceStatus.FAILED);
            closeQuietly();
            throw new ServiceException(NAME, "OpenSearch failed to start", failure);
        }
    }

    /**
     * Layers configuration in the order the supervisor's contract requires: the operator's
     * {@code opensearch.yml} first, then the supervisor's own settings, then the directory
     * layout, which is never negotiable — the paths belong to the process, not to OpenSearch.
     *
     * <p>The file is loaded explicitly rather than left to {@link InternalSettingsPreparer},
     * which only ever looks for a file literally named {@code opensearch.yml}; the supervisor
     * is free to point {@link ServiceContext#configFile()} somewhere else.
     */
    private Settings buildSettings(ServiceContext serviceContext) throws Exception {
        Settings.Builder builder = Settings.builder();
        Path configFile = serviceContext.configFile();
        if (configFile != null && Files.isReadable(configFile)) {
            builder.loadFromPath(configFile);
        }

        Map<String, String> supervisor = serviceContext.settings();
        put(builder, "cluster.name", supervisor.get(SETTING_CLUSTER_NAME));
        put(builder, "node.name", supervisor.get(SETTING_NODE_NAME));
        put(builder, "http.port", supervisor.get(SETTING_HTTP_PORT));
        put(builder, "transport.port", supervisor.get(SETTING_TRANSPORT_PORT));
        put(builder, "network.host", supervisor.get(SETTING_NETWORK_HOST));

        return builder
                .put("path.home", serviceContext.homeDirectory().toAbsolutePath().toString())
                .put("path.data", serviceContext.dataDirectory().toAbsolutePath().toString())
                .put("path.logs", serviceContext.logsDirectory().toAbsolutePath().toString())
                .build();
    }

    private static void put(Settings.Builder builder, String key, String value) {
        if (value != null && !value.isBlank()) {
            builder.put(key, value.trim());
        }
    }

    /**
     * Waits for the node to join a cluster and for health to reach YELLOW.
     *
     * <p>A cluster health request is a cluster-manager read, so it does not answer until this
     * node has discovered a cluster manager: one call covers both "has joined" and "has usable
     * shards". RED at the deadline is a failure — the supervisor must not report RUNNING while
     * primaries are unassigned.
     */
    private void awaitHealthy(Duration timeout) throws ServiceException {
        ClusterHealthResponse health = client.admin().cluster().prepareHealth()
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueMillis(timeout.toMillis()))
                .execute()
                // The health request honours its own timeout; the future gets a little more
                // rope so a hung transport surfaces as our error rather than hanging forever.
                .actionGet(timeout.toMillis() + TimeUnit.SECONDS.toMillis(10));
        if (health.isTimedOut() || health.getStatus() == ClusterHealthStatus.RED) {
            throw new ServiceException(NAME, "OpenSearch did not reach YELLOW within " + timeout
                    + ": status=" + health.getStatus()
                    + ", active shards=" + health.getActiveShards()
                    + ", unassigned shards=" + health.getUnassignedShards());
        }
    }

    @Override
    public void stop() throws Exception {
        Node stopping = node;
        // Drop every reference first: after this returns the supervisor discards the isolated
        // ClassLoader, and anything still reachable from here pins it and leaks a whole server
        // worth of classes and static state.
        node = null;
        client = null;
        clusterService = null;
        snapshot = null;
        if (stopping == null) {
            markStopped();
            return;
        }
        ServiceStatus previous = status.get();
        if (previous != ServiceStatus.DECOMMISSIONED) {
            status.set(ServiceStatus.STOPPING);
        }
        try {
            stopping.close();
            if (!stopping.awaitClose(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                status.set(ServiceStatus.FAILED);
                throw new ServiceException(NAME,
                        "OpenSearch node did not shut down within " + CLOSE_TIMEOUT_SECONDS + "s");
            }
        } catch (ServiceException e) {
            throw e;
        } catch (Throwable failure) {
            status.set(ServiceStatus.FAILED);
            throw new ServiceException(NAME, "OpenSearch failed to stop cleanly", failure);
        }
        markStopped();
    }

    /**
     * DECOMMISSIONED and FAILED survive {@code stop()}. Both say something STOPPED does not —
     * that this node handed its data off before leaving, or that it died rather than being
     * asked to leave — and the supervisor reports the outcome after the final stop.
     */
    private void markStopped() {
        status.updateAndGet(current ->
                current == ServiceStatus.DECOMMISSIONED || current == ServiceStatus.FAILED
                        ? current
                        : ServiceStatus.STOPPED);
    }

    /** Best-effort cleanup of a half-built node after a failed {@code start()}. */
    private void closeQuietly() {
        Node failed = node;
        node = null;
        client = null;
        clusterService = null;
        snapshot = null;
        if (failed == null) {
            return;
        }
        try {
            failed.close();
            failed.awaitClose(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // The original startup failure is the one worth reporting.
        }
    }

    // --- observation -----------------------------------------------------------------------

    @Override
    public ServiceStatus status() {
        return status.get();
    }

    @Override
    public Map<String, String> details() {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("status", status.get().name());
        if (nodeName != null) {
            details.put("node.name", nodeName);
        }
        if (nodeId != null) {
            details.put("node.id", nodeId);
        }
        if (httpAddress != null) {
            details.put("http.address", httpAddress);
            details.put("transport.address", transportAddress);
        }
        ClusterSnapshot current = currentSnapshot();
        if (current != null) {
            details.put("cluster.name", current.clusterName());
            details.put("cluster.health", current.healthStatus());
            details.put("shards.total", Integer.toString(current.shards()));
            details.put("shards.primaries", Integer.toString(current.primaries()));
            details.put("shards.relocating", Integer.toString(current.relocating()));
            details.put("shards.initializing", Integer.toString(current.initializing()));
        }
        return details;
    }

    /**
     * @return the derived view of the last applied cluster state, or null if the node is not
     *         running. Recomputed only when the cluster state version has moved on.
     */
    private ClusterSnapshot currentSnapshot() {
        ClusterService service = clusterService;
        if (service == null) {
            return null;
        }
        ClusterState state = service.state();
        ClusterSnapshot cached = snapshot;
        if (cached != null && cached.version() == state.version()) {
            return cached;
        }
        ClusterSnapshot fresh = ClusterSnapshot.of(state, nodeId);
        snapshot = fresh;
        return fresh;
    }

    // --- decommission ----------------------------------------------------------------------

    @Override
    public void prepareDecommission(DecommissionContext decommission) throws Exception {
        ClusterService service = requireRunning("prepareDecommission");
        String exclusion = exclusionListIncludingThisNode(service.state());
        try {
            client.admin().cluster().prepareUpdateSettings()
                    // Transient, not persistent: this exclusion is about a node that is leaving
                    // now, and it must not outlive a full cluster restart and quietly keep a
                    // rebuilt node of the same name from ever holding shards again.
                    .setTransientSettings(Settings.builder().put(ALLOCATION_EXCLUDE_NAME, exclusion))
                    .execute()
                    .actionGet(TimeValue.timeValueMillis(decommission.timeout().toMillis()));
        } catch (Throwable failure) {
            throw new ServiceException(NAME, "failed to exclude " + nodeName + " from shard allocation", failure);
        }
        status.set(ServiceStatus.DECOMMISSIONING);
        decommission.reportProgress(0, "excluded " + nodeName + " from shard allocation");
    }

    /**
     * Appends this node to whatever exclusion list is already in force.
     *
     * <p>Overwriting the setting would silently re-admit any node an operator (or an earlier,
     * abandoned decommission) had already excluded, and the cluster would start moving shards
     * back onto a node someone is in the middle of retiring.
     */
    private String exclusionListIncludingThisNode(ClusterState state) {
        Set<String> names = new LinkedHashSet<>();
        String existing = state.metadata().settings().get(ALLOCATION_EXCLUDE_NAME);
        if (existing != null && !existing.isBlank()) {
            Arrays.stream(existing.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(names::add);
        }
        names.add(nodeName);
        return String.join(",", names);
    }

    @Override
    public boolean awaitDecommissionReady(DecommissionContext decommission) throws Exception {
        ClusterService service = requireRunning("awaitDecommissionReady");
        long deadline = System.nanoTime() + decommission.timeout().toNanos();
        int total = shardsOnThisNode(service.state());
        if (total == 0) {
            decommission.reportProgress(100, "no shards on this node");
            return true;
        }
        while (true) {
            if (decommission.isCancelled()) {
                decommission.reportProgress(-1, "decommission cancelled with "
                        + shardsOnThisNode(service.state()) + " shards still on this node");
                return false;
            }
            int remaining = shardsOnThisNode(service.state());
            if (remaining == 0) {
                decommission.reportProgress(100, "relocated all " + total + " shards");
                return true;
            }
            // Shards can also be created here between polls (an index created mid-drain), so
            // the denominator is the high-water mark rather than the first reading.
            total = Math.max(total, remaining);
            decommission.reportProgress(
                    (int) ((total - remaining) * 100L / total),
                    "relocating " + remaining + " of " + total + " shards");

            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                return false;
            }
            try {
                Thread.sleep(Math.min(DECOMMISSION_POLL_INTERVAL.toMillis(),
                        Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos))));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    @Override
    public void decommission(DecommissionContext decommission) throws Exception {
        ClusterService service = requireRunning("decommission");
        int remaining = shardsOnThisNode(service.state());
        if (remaining > 0) {
            if (!decommission.force()) {
                throw new ServiceException(NAME, "refusing to decommission " + nodeName + ": "
                        + remaining + " shards are still allocated here. Let"
                        + " awaitDecommissionReady() finish, or pass --force to accept losing"
                        + " any shard that has no other copy.");
            }
            context.reportEvent("WARN", "opensearch.decommission.forced",
                    "decommissioning " + nodeName + " with " + remaining
                            + " shards still allocated; unreplicated data on this node is lost");
        }
        status.set(ServiceStatus.DECOMMISSIONED);
        decommission.reportProgress(100, "node " + nodeName + " has left the cluster's allocation");
    }

    private int shardsOnThisNode(ClusterState state) {
        RoutingNode local = state.getRoutingNodes().node(nodeId);
        return local == null ? 0 : local.size();
    }

    private ClusterService requireRunning(String operation) throws ServiceException {
        ClusterService service = clusterService;
        if (service == null) {
            throw new ServiceException(NAME, operation + " requires a running node, but status is " + status.get());
        }
        return service;
    }

    // --- helpers ---------------------------------------------------------------------------

    private static Duration startTimeout(ServiceContext serviceContext) {
        String configured = serviceContext.settings().get(SETTING_START_TIMEOUT);
        if (configured == null || configured.isBlank()) {
            return DEFAULT_START_TIMEOUT;
        }
        return Duration.ofSeconds(Long.parseLong(configured.trim()));
    }

    /**
     * Fallback when neither the supervisor nor {@code opensearch.yml} names the node. In a real
     * deployment the supervisor supplies {@code node_name} derived from Cassandra's node
     * identity; this only keeps a bare configuration from failing.
     */
    private static String defaultNodeName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "opensearch-node";
        }
    }
}
