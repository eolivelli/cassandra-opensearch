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
import java.util.Locale;
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
    /**
     * The mode the fork leaves behind when {@code decommission()} threw <i>after</i>
     * {@code unbootstrap()}; see {@link #statusAfterFailedDecommission}.
     */
    private static final String MODE_DECOMMISSION_FAILED = "DECOMMISSION_FAILED";
    private static final Duration SHUTDOWN_STEP_TIMEOUT = Duration.ofMinutes(1);

    private final AtomicBoolean stopped = new AtomicBoolean();

    private volatile ServiceStatus status = ServiceStatus.NEW;
    private volatile ServiceContext context;
    private volatile CassandraDaemon daemon;
    private volatile NodeJmxServer jmx;
    private volatile InterceptingKiller killer;
    private volatile DelegatingUncaughtExceptionHandler uncaughtExceptionHandler;
    private volatile RingStateWatcher ringStateWatcher;

    /** Memoised {@code getOwnershipWithPort()}; see {@link #ownership(StorageService)}. */
    private volatile Ownership ownership;

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
        // Captured out here, not inside the try, because the failure path needs it: everything
        // between activate() and installUncaughtExceptionHandler() runs with Cassandra's own
        // handler installed process-wide and nothing holding the one it displaced.
        Thread.UncaughtExceptionHandler handlerBeforeCassandra = Thread.getDefaultUncaughtExceptionHandler();
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

            // Started only now that the node is NORMAL, so the watcher's baseline is a settled
            // ring rather than the JOINING churn of a bootstrap.
            ringStateWatcher = RingStateWatcher.start(
                    context, () -> StorageService.instance.getOperationMode());
        } catch (Throwable t) {
            status = ServiceStatus.FAILED;
            releaseProcessWideState(handlerBeforeCassandra);
            throw new ServiceException(NAME, "Cassandra node failed to start", t);
        }
    }

    /**
     * Hands back the two pieces of JVM-global state a failed start may have taken, neither of
     * which anything else will give back.
     *
     * <p>Both exist because the sequence above takes them from Cassandra and only tidies up after
     * {@code activate()} has returned; a failure in between — {@code setup()} throwing, the native
     * transport finding its port taken, {@code removeShutdownHooks()} itself throwing — leaves
     * them installed, outliving the node and pinning the isolated ClassLoader they came from.
     *
     * <ul>
     *   <li>{@code StorageService.initServer()} registers a drain-on-shutdown hook with the JVM.
     *       Left in place it runs on the supervisor's exit, out of order, against a node that
     *       never came up.</li>
     *   <li>{@code CassandraDaemon.setup()} sets the process-wide default uncaught-exception
     *       handler about half way through, and {@link #installUncaughtExceptionHandler} — the
     *       thing that remembers what it displaced — runs only afterwards. {@link #stop()}
     *       restores only a handler this class wrapped itself, so before that point there is
     *       nothing that knows how to take Cassandra's back off.</li>
     * </ul>
     *
     * <p>The handler is left alone once the wrapper is in place: from there {@link #stop()} owns
     * the restoration, and undoing it here would strip a handler the node is still using.
     */
    private void releaseProcessWideState(Thread.UncaughtExceptionHandler handlerBeforeCassandra) {
        try {
            // Safe when nothing was registered: the hook list is simply empty.
            JVMStabilityInspector.removeShutdownHooks();
        } catch (Throwable ignored) {
            // Best effort. The startup failure is the one worth reporting.
        }
        if (uncaughtExceptionHandler == null
                && Thread.getDefaultUncaughtExceptionHandler() != handlerBeforeCassandra) {
            Thread.setDefaultUncaughtExceptionHandler(handlerBeforeCassandra);
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
            details.put("ownership", ownership(storage));
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

    /**
     * Per-endpoint ring ownership, recomputed only when the ring itself has moved.
     *
     * <p>{@code getOwnershipWithPort()} is far too expensive to call on every poll:
     * {@code TokenMetadata.getRingVersion()} aside, it takes the token metadata read lock once per
     * token, which with the default 256 vnodes is 256 acquisitions per node in the ring — and
     * {@code details()} is on the SPI's "must be cheap" list precisely because the health monitor
     * polls it. The ring version changes on every membership or token change, so a cache keyed on
     * it is exact rather than merely fresh-enough.
     */
    private String ownership(StorageService storage) {
        long ringVersion = storage.getTokenMetadata().getRingVersion();
        Ownership cached = ownership;
        if (cached != null && cached.ringVersion() == ringVersion) {
            return cached.formatted();
        }
        Ownership fresh = new Ownership(ringVersion, formatOwnership(storage.getOwnershipWithPort()));
        ownership = fresh;
        return fresh.formatted();
    }

    /** One computed ownership string and the ring version it was computed from. */
    private record Ownership(long ringVersion, String formatted) {
    }

    private static String formatOwnership(Map<String, Float> ownership) {
        return ownership.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                // Locale.ROOT, not the default: this string is machine-read as much as it is
                // displayed, and under a locale such as fr_FR the default would render 100.0% as
                // "100,0%" — which then collides with the comma that separates the entries.
                .map(e -> e.getKey() + '=' + String.format(Locale.ROOT, "%.1f%%", e.getValue() * 100))
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

    /**
     * {@inheritDoc}
     *
     * <p>Nothing to undo, and that is a property of {@link #prepareDecommission} rather than an
     * omission: it only asks whether this node may leave and reports progress. The ring, the
     * gossip state and the operation mode are all untouched until {@link #decommission} calls
     * {@code StorageService.decommission()}, which is past the point of no return and never
     * compensated. The method exists so the supervisor can compensate uniformly, without having
     * to know which of its services actually mutated anything.
     */
    @Override
    public void abortDecommission(DecommissionContext request) {
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
            // Ask the node what actually happened rather than assuming it is still a ring member.
            // A throw from decommission() does not mean it stopped early: this fork runs its
            // decommission hooks after unbootstrap(), completes the whole sequence — leaveRing,
            // shutdownClientServers, Gossiper.stop, MessagingService.shutdown, Stage.shutdownNow,
            // setMode(DECOMMISSIONED) — and only then throws if a hook failed, deliberately, since
            // by then there is nothing left to roll back to. Reporting RUNNING on that path tells
            // the operator this node is still serving a ring it has already left.
            String mode = StorageService.instance.getOperationMode();
            status = statusAfterFailedDecommission(mode);
            throw new ServiceException(NAME,
                    MODE_DECOMMISSIONED.equals(mode)
                            ? "Decommission reported a failure after the node had already left the"
                                    + " ring; operation mode is " + mode + ". The node is out of the"
                                    + " cluster and cannot rejoin without bootstrapping."
                            : "Decommission failed; operation mode is now " + mode, t);
        }

        String mode = StorageService.instance.getOperationMode();
        if (!MODE_DECOMMISSIONED.equals(mode)) {
            status = statusAfterFailedDecommission(mode);
            throw new ServiceException(NAME,
                    "Decommission returned without leaving the ring; operation mode is " + mode);
        }
        status = ServiceStatus.DECOMMISSIONED;
        request.reportProgress(100, "left the ring");
        // Not ring.state.changed: that event now belongs to RingStateWatcher, which has already
        // reported this very transition off its own poll. This one says something the mode alone
        // does not — that the node left because the supervisor asked it to.
        context.reportEvent("INFO", "cassandra.decommissioned", "this node has left the ring");
    }

    /**
     * What to report once a decommission has ended in something other than success.
     *
     * <p>Three answers, and the middle one is the reason this is a method rather than a ternary:
     *
     * <ul>
     *   <li>{@code DECOMMISSIONED} — it worked; only a hook or a shutdown step complained.</li>
     *   <li>{@code DECOMMISSION_FAILED} — <b>not</b> RUNNING. The fork sets this mode from the
     *       catch blocks at the very bottom of {@code StorageService.decommission()}, every one of
     *       which sits <i>after</i> {@code unbootstrap()} has already called {@code leaveRing()}:
     *       the endpoint is out of {@code TokenMetadata}, LEFT has been gossiped and the bootstrap
     *       state is persisted. A bare {@code InterruptedException}, or a throw from
     *       {@code shutdownClientServers()}, {@code Gossiper.stop()} or {@code Stage.shutdownNow()},
     *       all land here. The node holds no ranges and is serving nothing; calling that RUNNING is
     *       the same lie about the same code path that the {@code DECOMMISSIONED} branch above
     *       exists to prevent. FAILED also survives {@link #markStopped()}, so the post-mortem
     *       reaches the operator after the supervisor's final stop.</li>
     *   <li>anything else — NORMAL, LEAVING, MOVING: the ring is intact and the node really is
     *       still serving, so RUNNING is the truth.</li>
     * </ul>
     */
    private static ServiceStatus statusAfterFailedDecommission(String mode) {
        if (MODE_DECOMMISSIONED.equals(mode)) {
            return ServiceStatus.DECOMMISSIONED;
        }
        return MODE_DECOMMISSION_FAILED.equals(mode) ? ServiceStatus.FAILED : ServiceStatus.RUNNING;
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
        // First, and before drain(): every mode this teardown is about to walk through is one the
        // supervisor asked for, and reporting DRAINING as a ring event would be noise at best.
        if (ringStateWatcher != null) {
            step("ring state watcher", () -> ringStateWatcher.stop());
        }
        if (daemon == null) {
            // start() failed before the daemon existed; only JMX can be holding anything.
            if (jmx != null) {
                step("stop JMX", () -> jmx.stop());
            }
            markStopped();
            return;
        }
        if (!isTerminalOutcome(status)) {
            status = ServiceStatus.STOPPING;
        }

        // Before every step that shuts something down, and in particular before drain(). The
        // GCInspector this node registered is a listener on the JVM's own garbage-collector
        // MXBeans, and on an old-generation collection it submits to an executor drain() has
        // already stopped; the platform beans dispatch listeners in a bare loop, so the
        // RejectedExecutionException that follows also silences every listener behind it. Taking
        // it off first closes that window instead of leaving it open for the whole teardown.
        if (jmx != null) {
            step("detach GCInspector", () -> jmx.detachGcInspector());
        }

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

        markStopped();
        if (context != null) {
            context.reportEvent("INFO", "cassandra.stopped", "Cassandra node stopped");
        }
    }

    /**
     * DECOMMISSIONED and FAILED survive {@code stop()}, exactly as they do in the OpenSearch
     * runtime. Both say something STOPPED does not — that this node handed its data off before
     * leaving, or that it died rather than being asked to leave — and the supervisor reports the
     * outcome after the final stop, when every service has already been stopped. Overwriting them
     * with STOPPED turns a decommissioned node into one that merely went away, and loses the
     * post-mortem of a node that failed.
     *
     * <p>Not atomic, and does not need to be: every write to {@code status} happens on the
     * supervisor's single lifecycle thread.
     */
    private void markStopped() {
        if (!isTerminalOutcome(status)) {
            status = ServiceStatus.STOPPED;
        }
    }

    /** @return true for the two states {@code stop()} must not overwrite */
    private static boolean isTerminalOutcome(ServiceStatus current) {
        return current == ServiceStatus.DECOMMISSIONED || current == ServiceStatus.FAILED;
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
