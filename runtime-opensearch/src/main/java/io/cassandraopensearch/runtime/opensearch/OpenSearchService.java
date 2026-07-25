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
 * exception handler that calls {@link System#exit}, memory-locks and initialises JNA natives,
 * and runs {@code LogConfigurator}, which hijacks {@code java.util.logging} for the whole JVM
 * and redirects {@code System.out} and {@code System.err} into log4j2 — swallowing Cassandra's
 * console output along the way. None of it fires when the node is constructed by hand.
 *
 * <p>For the same reason no {@code io.netty.*} system property is set here even though
 * OpenSearch's own launcher sets several: those are read by Cassandra's Netty out of the same
 * JVM, and {@code io.netty.noUnsafe=true} in particular measurably slows Cassandra's native
 * transport.
 *
 * <h2>Restarting</h2>
 *
 * {@link #start} may be called again after {@link #stop}, and that is the only supported way to
 * restart: the isolated ClassLoader does not become collectable once a node has closed, so
 * discarding it and building a new one leaks a server's worth of metaspace, while a fresh
 * {@link Node} inside the existing loader comes up in a fraction of a cold start.
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

    /**
     * Upper bound on a {@code _cluster/settings} update, independent of the decommission's own
     * deadline. {@code prepareDecommission} and {@code abortDecommission} must return promptly —
     * the long wait belongs in {@code awaitDecommissionReady} — and a decommission timeout is
     * sized for relocating shards (tens of minutes), not for a cluster-manager write.
     */
    private static final Duration SETTINGS_UPDATE_TIMEOUT = Duration.ofSeconds(30);

    /** Cluster-wide allocation filter; setting it to our node name drains this node's shards. */
    private static final String ALLOCATION_EXCLUDE_NAME = "cluster.routing.allocation.exclude._name";

    /** Installs a JVM-wide deserialization filter; see {@link #nodeEnvironment}. */
    private static final String BOOTSTRAP_SERIAL_FILTER = "bootstrap.serial_filter";

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
            Environment environment = nodeEnvironment(serviceContext);
            this.nodeName = Node.NODE_NAME_SETTING.get(environment.settings());

            Node started = new EmbeddedNode(environment, List.of(EmbeddedNode.classpathPlugin(Netty4ModulePlugin.class)));
            this.node = started;
            started.start();

            this.client = started.client();
            // nodeId before clusterService, not the other way round: currentSnapshot() keys its
            // memoised snapshot off clusterService being non-null and reads nodeId to find this
            // node's shards, so publishing the service first lets a concurrent details() cache a
            // snapshot built against a null node id and serve it until the cluster state moves.
            ClusterService service = started.injector().getInstance(ClusterService.class);
            this.nodeId = service.localNode().getId();
            this.clusterService = service;
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
     * The {@link Environment} {@link #start} hands to the {@link Node}: the layered settings, run
     * through {@link InternalSettingsPreparer}, with {@code bootstrap.serial_filter} taken back
     * out.
     *
     * <p>Package-private so a test can assert on exactly what the node is constructed with.
     */
    Environment nodeEnvironment(ServiceContext serviceContext) throws Exception {
        Environment prepared = InternalSettingsPreparer.prepareEnvironment(
                buildSettings(serviceContext),
                Map.of(),
                serviceContext.configDirectory(),
                OpenSearchService::defaultNodeName);
        return withoutSerialFilter(prepared, serviceContext);
    }

    /**
     * Removes {@code bootstrap.serial_filter} from an assembled environment.
     *
     * <p>The setting is not OpenSearch's to enable here: where it exists, {@code Node.<init>}
     * turns it into a process-wide {@link java.io.ObjectInputFilter} that rejects every Java
     * deserialization, which would break Cassandra's JMX/RMI and internal serialization in the
     * same JVM. It is dropped rather than set to {@code false}, because {@code opensearch-3.7.0}
     * does not declare it at all — {@code BootstrapSettings} has only
     * {@code SECURITY_FILTER_BAD_DEFAULTS}, {@code MEMORY_LOCK}, {@code SYSTEM_CALL_FILTER} and
     * {@code CTRLHANDLER}, and no class in the jar so much as mentions {@code serial_filter} — so
     * on today's version leaving it in is an {@code unknown setting} start failure, and on a
     * future version that knows it, it is the hazard above. Dropping it covers both.
     *
     * <p>This happens <b>after</b> {@link InternalSettingsPreparer}, and that is the whole point.
     * Stripping the key from the builder handed to {@code prepareEnvironment} neutralises nothing:
     * that method discards the assembled builder, re-reads {@code <configDir>/opensearch.yml} as
     * the base layer and puts our settings on top, and {@code Settings.Builder.put(Settings)} is a
     * {@code putAll} — additive, with no way to express a removal. A key present in the operator's
     * file survives the round trip; only a removal applied to {@code environment.settings()}
     * afterwards actually removes it.
     */
    private static Environment withoutSerialFilter(Environment prepared, ServiceContext serviceContext) {
        if (!prepared.settings().hasValue(BOOTSTRAP_SERIAL_FILTER)) {
            return prepared;
        }
        Settings.Builder stripped = Settings.builder().put(prepared.settings());
        stripped.remove(BOOTSTRAP_SERIAL_FILTER);
        serviceContext.reportEvent("WARN", "opensearch.setting.ignored",
                BOOTSTRAP_SERIAL_FILTER + " installs a JVM-wide deserialization filter that would"
                        + " break Cassandra in this process; the setting has been dropped");
        // Same config directory, so every path Environment derives comes out identical; only the
        // settings map differs.
        return new Environment(stripped.build(), prepared.configDir());
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
        // Drop every reference first, and make the second call a no-op while doing it: a stale
        // Node kept alive by this object would hold its thread pools, page caches and mapped
        // files long after the supervisor believes the service is gone.
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
        // The identity fields go too. They are set one at a time as start() gets further, so after
        // a failure they describe a node that never came up — and details() would advertise its
        // name, id and listen addresses next to a status of FAILED, which reads as a node that is
        // there and merely unhealthy. Nothing is listening on those addresses.
        nodeName = null;
        nodeId = null;
        httpAddress = null;
        transportAddress = null;
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

    /**
     * {@inheritDoc}
     *
     * <p>Callable from any thread at any point in the lifecycle, so nothing in here may propagate:
     * a node being closed underneath this call can throw from {@code ClusterService.state()}, and
     * the health monitor that polls this is not the place to find out.
     */
    @Override
    public Map<String, String> details() {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("status", status.get().name());
        try {
            String name = nodeName;
            if (name != null) {
                details.put("node.name", name);
            }
            String id = nodeId;
            if (id != null) {
                details.put("node.id", id);
            }
            String http = httpAddress;
            if (http != null) {
                details.put("http.address", http);
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
        } catch (Throwable failure) {
            details.put("detailsError", failure.toString());
        }
        return details;
    }

    /**
     * @return the derived view of the last applied cluster state, or null if the node is not
     *         running. Recomputed only when the applied state has actually changed.
     */
    private ClusterSnapshot currentSnapshot() {
        ClusterService service = clusterService;
        if (service == null) {
            return null;
        }
        ClusterState state = service.state();
        ClusterSnapshot cached = snapshot;
        if (cached != null && cached.stateUuid().equals(state.stateUUID())) {
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
                    .actionGet(settingsUpdateTimeout(decommission));
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
        Set<String> names = currentExclusions(state);
        names.add(nodeName);
        return String.join(",", names);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Re-admits this node to shard allocation. Only this node's name is taken out of the
     * exclusion list, for the same reason {@link #exclusionListIncludingThisNode} adds to it
     * rather than overwriting: someone else may be retiring another node, and re-admitting
     * <i>that</i> one would start moving shards back onto a node that is on its way out.
     *
     * <p>Clearing the setting outright when nothing else is excluded, rather than writing an
     * empty value, is what makes {@code _cluster/settings} come back to the shape it had before
     * the decommission was attempted — an operator reading it must not find the trace of an
     * exclusion that is no longer in force.
     */
    @Override
    public void abortDecommission(DecommissionContext decommission) throws Exception {
        ClusterService service = clusterService;
        if (service == null) {
            // Nothing was applied: prepareDecommission requires a running node and this is called
            // for every service the supervisor touched, including ones it never got to.
            return;
        }
        Set<String> remaining = currentExclusions(service.state());
        if (remaining.remove(nodeName)) {
            Settings.Builder update = Settings.builder();
            if (remaining.isEmpty()) {
                update.putNull(ALLOCATION_EXCLUDE_NAME);
            } else {
                update.put(ALLOCATION_EXCLUDE_NAME, String.join(",", remaining));
            }
            try {
                client.admin().cluster().prepareUpdateSettings()
                        .setTransientSettings(update)
                        .execute()
                        .actionGet(settingsUpdateTimeout(decommission));
            } catch (Throwable failure) {
                throw new ServiceException(NAME,
                        "failed to re-admit " + nodeName + " to shard allocation; the node stays"
                                + " excluded and will not be given a shard until the exclusion is"
                                + " removed by hand", failure);
            }
        }
        status.compareAndSet(ServiceStatus.DECOMMISSIONING, ServiceStatus.RUNNING);
        decommission.reportProgress(-1, "re-admitted " + nodeName + " to shard allocation");
    }

    /**
     * The names in the <i>transient</i> {@code cluster.routing.allocation.exclude._name}, in the
     * order they appear.
     *
     * <p>Deliberately not {@code metadata().settings()}, which is the persistent and transient
     * layers merged. Reading the merged view means copying whatever an operator put in the
     * persistent layer into the transient one, and the transient copy then outlives its source:
     * remove the persistent entry and the node it named stays excluded, by a setting nobody wrote
     * and nothing will clear. This service writes only the transient layer, so it reads only the
     * transient layer, and {@link #abortDecommission} leaves it exactly as it found it.
     *
     * <p>The cost is that a <i>persistent</i> exclusion for another node is shadowed for as long
     * as ours is in force, because transient wins over persistent for the same key. That window is
     * the decommission, it is visible in {@code _cluster/settings}, and it ends when this node's
     * exclusion is cleared — which is a better trade than a transient entry with no owner.
     *
     * <p>Best-effort in one further respect: this reads the locally applied cluster state, which
     * can lag the cluster manager. A concurrent exclusion applied elsewhere in the same instant is
     * not visible here and would be overwritten by the update that follows.
     */
    private static Set<String> currentExclusions(ClusterState state) {
        Set<String> names = new LinkedHashSet<>();
        String existing = state.metadata().transientSettings().get(ALLOCATION_EXCLUDE_NAME);
        if (existing != null && !existing.isBlank()) {
            Arrays.stream(existing.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(names::add);
        }
        return names;
    }

    /**
     * @return how long to wait for a {@code _cluster/settings} update, never more than
     *         {@link #SETTINGS_UPDATE_TIMEOUT} however long the decommission itself may run
     */
    private static TimeValue settingsUpdateTimeout(DecommissionContext decommission) {
        long millis = Math.min(decommission.timeout().toMillis(), SETTINGS_UPDATE_TIMEOUT.toMillis());
        return TimeValue.timeValueMillis(Math.max(1, millis));
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
