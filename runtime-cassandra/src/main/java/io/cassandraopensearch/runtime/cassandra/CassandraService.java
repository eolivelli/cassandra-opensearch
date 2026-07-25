/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.cassandraopensearch.spi.DecommissionContext;
import io.cassandraopensearch.spi.EmbeddedService;
import io.cassandraopensearch.spi.ServiceContext;
import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.memory.BufferPools;
import org.slf4j.LoggerFactory;

/**
 * A real Cassandra node, embedded in a JVM it does not own.
 *
 * <p>This class is loaded exclusively by the {@code cassandra} child ClassLoader and never appears
 * on the supervisor's classpath — see {@code docs/ARCHITECTURE.md}. It exists to make three things
 * true that are not true of a stock {@code CassandraDaemon}:
 *
 * <ul>
 *   <li><b>Cassandra cannot end the process.</b> {@code new CassandraDaemon(true)} makes
 *       {@code exitOrFail()} throw, and {@link InterceptingKiller} covers the entirely separate
 *       {@code JVMStabilityInspector.Killer.killJVM()} path that {@code runManaged} does not touch.</li>
 *   <li><b>Cassandra owns no JVM-global lifecycle.</b> The drain-on-shutdown hook that
 *       {@code StorageService.initServer()} registers is removed after startup, and the
 *       process-wide uncaught-exception handler is wrapped rather than replaced.</li>
 *   <li><b>Its MBeans stay out of the platform namespace</b>, while the stock {@code nodetool}
 *       still works — see {@link NodeJmxServer} and {@link FederatedMBeanServer}.</li>
 * </ul>
 *
 * <p>The startup ordering below is not stylistic; each constraint was established by the spike
 * recorded in {@code docs/spikes/cassandra-embedding.md} and is called out where it applies.
 */
public final class CassandraService implements EmbeddedService {

    /** Stable service name, matching {@code lib/cassandra/} and the supervisor's configuration. */
    public static final String NAME = "cassandra";

    /**
     * Supervisor settings this service understands. Looked up both bare and under a
     * {@code cassandra.} prefix, since {@code conf/cassandra-opensearch.yaml} may be flattened
     * either per-service or whole.
     */
    static final String SETTING_JMX_ADDRESS = "jmx.address";
    static final String SETTING_JMX_PORT = "jmx.port";
    static final String SETTING_STARTUP_TIMEOUT_SECONDS = "startup.timeout.seconds";

    private static final String MODE_NORMAL = "NORMAL";
    private static final String MODE_DECOMMISSIONED = "DECOMMISSIONED";
    private static final Duration SHUTDOWN_STEP_TIMEOUT = Duration.ofMinutes(1);

    private final AtomicBoolean stopped = new AtomicBoolean();

    private volatile ServiceStatus status = ServiceStatus.NEW;
    private volatile ServiceContext context;
    private volatile CassandraDaemon daemon;
    private volatile NodeJmxServer jmx;
    private volatile InterceptingKiller killer;
    private volatile DelegatingUncaughtExceptionHandler uncaughtExceptionHandler;

    @Override
    public String name() {
        return NAME;
    }

    // --- startup ---------------------------------------------------------------------------

    @Override
    public void start(ServiceContext context) throws Exception {
        if (status != ServiceStatus.NEW) {
            throw new ServiceException(NAME, "start() may only be called once; service is " + status);
        }
        this.context = context;
        this.status = ServiceStatus.STARTING;
        try {
            applySystemProperties(context);

            // Before activate(): the killer must be in place before anything can trip it, and the
            // JMX bring-up is what forces MBeanWrapper.instance to resolve to our federated
            // wrapper, which GCInspector reads during CassandraDaemon.setup().
            killer = new InterceptingKiller(context);
            JVMStabilityInspector.replaceKiller(killer);

            String jmxAddress = setting(context, SETTING_JMX_ADDRESS, "127.0.0.1");
            int jmxPort = Integer.parseInt(setting(context, SETTING_JMX_PORT, "7199"));
            jmx = NodeJmxServer.start(jmxAddress, jmxPort);
            context.reportEvent("INFO", "cassandra.jmx.started", jmx.serviceUrl());

            Thread.UncaughtExceptionHandler handlerBeforeCassandra = Thread.getDefaultUncaughtExceptionHandler();

            // runManaged=true so exitOrFail() throws rather than calling System.exit.
            daemon = new CassandraDaemon(true);
            daemon.activate();

            // StorageService.initServer() registers a JVM-global drain-on-shutdown hook, and
            // PathUtils registers one per deleteOnExit request. Both would run on the supervisor's
            // exit, out of order and after the loader was meant to be discarded.
            JVMStabilityInspector.removeShutdownHooks();

            installUncaughtExceptionHandler(handlerBeforeCassandra);

            Duration timeout = Duration.ofSeconds(
                    Long.parseLong(setting(context, SETTING_STARTUP_TIMEOUT_SECONDS, "300")));
            awaitNormal(timeout);

            status = ServiceStatus.RUNNING;
            context.reportEvent("INFO", "cassandra.started",
                    "node " + StorageService.instance.getLocalHostId() + " is NORMAL in cluster "
                            + StorageService.instance.getClusterName());
        } catch (Throwable t) {
            status = ServiceStatus.FAILED;
            throw new ServiceException(NAME, "Cassandra node failed to start", t);
        }
    }

    /**
     * Sets the JVM-global properties Cassandra reads at boot.
     *
     * <p>Everything here is either {@code cassandra.}-namespaced or specific enough not to collide
     * with OpenSearch. In particular no {@code io.netty.*} property is set: both servers' Netty
     * copies read the same JVM globals, so those values belong on the launcher's command line where
     * they can be reconciled once — see {@code docs/spikes/cassandra-embedding.md}.
     */
    private void applySystemProperties(ServiceContext context) {
        System.setProperty("cassandra.config", context.configFile().toUri().toString());
        System.setProperty("cassandra.storagedir", context.dataDirectory().toString());
        System.setProperty("cassandra.logdir", context.logsDirectory().toString());
        System.setProperty("org.apache.cassandra.mbean_registration_class", NodeMBeanWrapper.class.getName());

        // Not a typo and not cassandra.-prefixed: without it activate() calls System.out.close()
        // and System.err.close() and the whole process, OpenSearch included, goes silent.
        System.setProperty("cassandra-foreground", "yes");

        // Leaving these unset is what makes CassandraDaemon.maybeInitJmx() a no-op, so that
        // NodeJmxServer owns the connector and can hand it the federated MBeanServer.
        System.clearProperty("cassandra.jmx.local.port");
        System.clearProperty("cassandra.jmx.remote.port");
    }

    private void installUncaughtExceptionHandler(Thread.UncaughtExceptionHandler handlerBeforeCassandra) {
        Thread.UncaughtExceptionHandler cassandraHandler = Thread.getDefaultUncaughtExceptionHandler();
        uncaughtExceptionHandler = new DelegatingUncaughtExceptionHandler(
                getClass().getClassLoader(), cassandraHandler, handlerBeforeCassandra);
        Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
    }

    private void awaitNormal(Duration timeout) throws InterruptedException, ServiceException {
        long deadline = System.nanoTime() + timeout.toNanos();
        String mode = StorageService.instance.getOperationMode();
        while (!MODE_NORMAL.equals(mode)) {
            if (System.nanoTime() - deadline > 0) {
                throw new ServiceException(NAME,
                        "Cassandra did not reach NORMAL within " + timeout.toSeconds()
                                + "s; operation mode is " + mode);
            }
            Thread.sleep(200);
            mode = StorageService.instance.getOperationMode();
        }
    }

    // --- observation -----------------------------------------------------------------------

    @Override
    public ServiceStatus status() {
        return status;
    }

    @Override
    public Map<String, String> details() {
        Map<String, String> details = new LinkedHashMap<>();
        details.put("status", status.name());
        if (daemon == null || stopped.get()) {
            return details;
        }
        try {
            StorageService storage = StorageService.instance;
            details.put("clusterName", storage.getClusterName());
            details.put("hostId", storage.getLocalHostId());
            details.put("operationMode", storage.getOperationMode());
            details.put("releaseVersion", storage.getReleaseVersion());
            details.put("joined", String.valueOf(storage.isJoined()));
            details.put("listenAddress",
                    DatabaseDescriptor.getListenAddress().getHostAddress() + ':' + DatabaseDescriptor.getStoragePort());
            details.put("nativeAddress",
                    DatabaseDescriptor.getRpcAddress().getHostAddress() + ':' + DatabaseDescriptor.getNativeTransportPort());
            details.put("gossipActive", String.valueOf(storage.isGossipRunning()));
            details.put("nativeTransportActive", String.valueOf(storage.isNativeTransportRunning()));
            details.put("load", storage.getLoadString());
            details.put("liveNodes", String.join(",", storage.getLiveNodes()));
            details.put("ownership", formatOwnership(storage.getOwnershipWithPort()));
            details.put("jmx", jmx == null ? "not started" : jmx.serviceUrl());
            Throwable lastKill = killer == null ? null : killer.lastKillAttempt();
            if (lastKill != null) {
                details.put("interceptedJvmKill", lastKill.toString());
            }
        } catch (Throwable t) {
            // details() is polled for health output and must never propagate; a node mid-shutdown
            // can throw from any of the calls above.
            details.put("detailsError", t.toString());
        }
        return details;
    }

    private static String formatOwnership(Map<String, Float> ownership) {
        return ownership.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + '=' + String.format("%.1f%%", e.getValue() * 100))
                .collect(Collectors.joining(","));
    }

    // --- decommission ----------------------------------------------------------------------

    @Override
    public void prepareDecommission(DecommissionContext request) throws Exception {
        requireRunning("prepareDecommission");
        rejectSoleRingMember();
        request.reportProgress(0, "Cassandra is ready to leave the ring");
    }

    /**
     * {@inheritDoc}
     *
     * <p>Cassandra has no separate hand-off phase: {@code StorageService.decommission()} streams
     * its ranges synchronously and only returns once the data is gone. So there is nothing to
     * pre-stage here, and this returns immediately — its purpose is to let the supervisor overlap
     * OpenSearch's shard relocation with Cassandra's, which for Cassandra means "start now".
     *
     * <p>The one case that does wait is a decommission already in flight, started by someone
     * running {@code nodetool decommission} straight against the {@code StorageService} MBean.
     */
    @Override
    public boolean awaitDecommissionReady(DecommissionContext request) throws Exception {
        long deadline = System.nanoTime() + request.timeout().toNanos();
        while (StorageService.instance.isDecommissioning()) {
            if (request.isCancelled() || System.nanoTime() - deadline > 0) {
                return false;
            }
            request.reportProgress(-1, "waiting for a decommission started outside the supervisor");
            Thread.sleep(500);
        }
        return true;
    }

    @Override
    public void decommission(DecommissionContext request) throws Exception {
        requireRunning("decommission");
        rejectSoleRingMember();

        status = ServiceStatus.DECOMMISSIONING;
        request.reportProgress(-1, "streaming ranges to the rest of the ring");
        try {
            StorageService.instance.decommission(request.force());
        } catch (Throwable t) {
            // The node is still up and still serving clients; only its operation mode records the
            // failure, as DECOMMISSION_FAILED rather than a revert to NORMAL.
            status = ServiceStatus.RUNNING;
            throw new ServiceException(NAME,
                    "Decommission failed; operation mode is now "
                            + StorageService.instance.getOperationMode(), t);
        }

        String mode = StorageService.instance.getOperationMode();
        if (!MODE_DECOMMISSIONED.equals(mode)) {
            status = ServiceStatus.RUNNING;
            throw new ServiceException(NAME,
                    "Decommission returned without leaving the ring; operation mode is " + mode);
        }
        status = ServiceStatus.DECOMMISSIONED;
        request.reportProgress(100, "left the ring");
        context.reportEvent("INFO", "ring.state.changed", "this node has been decommissioned");
    }

    /**
     * Refuses a decommission that Cassandra is going to refuse anyway, with an explanation.
     *
     * <p>{@code StorageService.decommission} rejects a single-node ring outright, and {@code force}
     * does not help because that guard is evaluated before the flag is even looked at. Left to
     * itself the caller gets a bare {@code UnsupportedOperationException} across the ClassLoader
     * boundary; this turns it into something an operator can act on.
     */
    private void rejectSoleRingMember() throws ServiceException {
        Set<InetAddressAndPort> remaining =
                StorageService.instance.getTokenMetadata().cloneAfterAllLeft().getAllEndpoints();
        if (remaining.size() < 2 && remaining.contains(FBUtilities.getBroadcastAddressAndPort())) {
            throw new ServiceException(NAME,
                    "Cannot decommission: this node is the only member of the ring, so there is"
                            + " nowhere to hand its data to. --force does not help, because Cassandra"
                            + " applies this check before it considers the flag. Stop the process"
                            + " instead — stop() drains the node — or grow the cluster first.");
        }
    }

    private void requireRunning(String operation) throws ServiceException {
        if (status != ServiceStatus.RUNNING) {
            throw new ServiceException(NAME, operation + " requires a running node; service is " + status);
        }
    }

    // --- shutdown --------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     *
     * <p>The order below is the one the spike measured to leave no non-daemon threads behind, and
     * it is load-bearing in one place in particular: JMX goes down early, while the executors are
     * still alive, because Cassandra keeps registering and unregistering metrics MBeans right
     * through its own teardown. That is safe only because {@link NodeMBeanWrapper} becomes inert
     * on close instead of failing.
     *
     * <p>Every step is best-effort. A failure in one must not strand the ones after it, which are
     * the steps that own threads and native memory.
     */
    @Override
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        if (daemon == null) {
            // start() failed before the daemon existed; only JMX can be holding anything.
            if (jmx != null) {
                step("stop JMX", () -> jmx.stop());
            }
            status = ServiceStatus.STOPPED;
            return;
        }
        status = ServiceStatus.STOPPING;

        step("stop client transports", () -> daemon.destroyClientTransports());
        step("drain", () -> StorageService.instance.drain());
        step("stop JMX", () -> jmx.stop());

        long timeout = SHUTDOWN_STEP_TIMEOUT.toMillis();
        step("messaging service", () -> MessagingService.instance().shutdown(timeout, TimeUnit.MILLISECONDS, false, true));
        step("stages", () -> Stage.shutdownAndWait(timeout, TimeUnit.MILLISECONDS));
        step("scheduled executors", () -> ScheduledExecutors.shutdownNowAndWait(timeout, TimeUnit.MILLISECONDS));
        step("column family stores", () -> ColumnFamilyStore.shutdownExecutorsAndWait(timeout, TimeUnit.MILLISECONDS));
        step("buffer pools", () -> BufferPools.shutdownLocalCleaner(timeout, TimeUnit.MILLISECONDS));
        step("reference reaper", () -> Ref.shutdownReferenceReaper(timeout, TimeUnit.MILLISECONDS));
        step("sstable readers", () -> SSTableReader.shutdownBlocking(timeout, TimeUnit.MILLISECONDS));
        step("shared executor pool", () -> SharedExecutorPool.SHARED.shutdownAndWait(timeout, TimeUnit.MILLISECONDS));
        step("commit log", () -> CommitLog.instance.shutdownBlocking());

        // PathUtils keeps its own family of JVM-global shutdown hooks for deleteOnExit bookkeeping.
        // Run them now and drop them, so nothing of Cassandra's is left attached to the JVM.
        step("delete-on-exit hooks", PathUtils::runOnExitThreadsAndClear);

        step("uncaught exception handler", this::restoreUncaughtExceptionHandler);
        step("logging", CassandraService::stopLoggerContext);

        status = ServiceStatus.STOPPED;
        if (context != null) {
            context.reportEvent("INFO", "cassandra.stopped", "Cassandra node stopped");
        }
    }

    private void restoreUncaughtExceptionHandler() {
        DelegatingUncaughtExceptionHandler installed = uncaughtExceptionHandler;
        if (installed != null && Thread.getDefaultUncaughtExceptionHandler() == installed) {
            Thread.setDefaultUncaughtExceptionHandler(installed.previousHandler());
        }
    }

    /**
     * Stops this node's own logging. Logback and slf4j live only in the isolated loader, so this
     * cannot reach OpenSearch's log4j2 — but it is reflective anyway, to avoid a compile-time
     * dependency on a binding that is a runtime choice.
     */
    private static void stopLoggerContext() throws Exception {
        Object loggerFactory = LoggerFactory.getILoggerFactory();
        loggerFactory.getClass().getMethod("stop").invoke(loggerFactory);
    }

    @FunctionalInterface
    private interface TeardownStep {
        void run() throws Exception;
    }

    private void step(String description, TeardownStep step) {
        try {
            step.run();
        } catch (Throwable t) {
            // Resolved on demand rather than held in a static field: a static Logger would
            // initialise logback when this class is loaded, which is before start() has had a
            // chance to set cassandra.logdir, and the node's log file would land in the wrong place.
            LoggerFactory.getLogger(CassandraService.class)
                    .warn("Cassandra shutdown step '{}' failed; continuing", description, t);
            if (context != null) {
                context.reportEvent("WARN", "cassandra.shutdown.step.failed", description + ": " + t);
            }
        }
    }

    // --- settings --------------------------------------------------------------------------

    private static String setting(ServiceContext context, String key, String defaultValue) {
        Map<String, String> settings = context.settings();
        String value = settings.get(NAME + '.' + key);
        if (value == null) {
            value = settings.get(key);
        }
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }
}
