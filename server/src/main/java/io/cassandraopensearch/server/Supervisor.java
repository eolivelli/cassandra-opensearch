/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.server.config.NodeConfiguration;
import io.cassandraopensearch.server.config.ServiceConfiguration;
import io.cassandraopensearch.spi.EmbeddedService;
import io.cassandraopensearch.spi.ServiceContext;
import io.cassandraopensearch.spi.ServiceStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Gives the two embedded servers one lifecycle.
 *
 * <h2>Order</h2>
 *
 * Startup is Cassandra, then OpenSearch, because OpenSearch's node identity derives from the
 * Cassandra node's host id — see {@link #derivedSettings}. Shutdown reverses it: OpenSearch is
 * the dependent service and goes first. Decommission is the same asymmetry made explicit, in
 * {@link DecommissionCoordinator}.
 *
 * <h2>The single shutdown hook</h2>
 *
 * This class owns the only shutdown hook in the process, and that is a hard constraint rather
 * than a tidiness preference. Cassandra's {@code StorageService.initServer()} registers a
 * JVM-global drain hook and {@code PathUtils} registers more; OpenSearch's {@code Bootstrap}
 * registers its own. All of them would fire on JVM exit in an order nobody chose, after the
 * supervisor believed the isolated loaders had been discarded. Both runtimes therefore remove or
 * never register theirs, and this one hook runs the ordered shutdown instead. {@link #stop()} is
 * idempotent with it: whichever gets there first does the work, the other returns immediately.
 *
 * <h2>System.exit</h2>
 *
 * Nothing here calls it, including the fatal-error path. A supervisor that killed the JVM
 * directly would skip the ordered shutdown it exists to guarantee, and would make this class
 * untestable. Failures are recorded as an exit code, {@link #awaitShutdown()} returns it, and
 * {@code main} is the only place that acts on it.
 */
public final class Supervisor implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Supervisor.class);

    /** Where {@code bin/cassandra-opensearch status|decommission} finds the running process. */
    public static final String MBEAN_NAME = "io.cassandraopensearch:type=Supervisor";

    /**
     * Added to the configured startup timeout when bounding {@code start()} from outside. Both
     * runtimes enforce the same limit internally and say something useful when they hit it —
     * which operation mode Cassandra is stuck in, how many shards OpenSearch has unassigned. The
     * grace lets that diagnosis surface instead of a bare "did not complete in time".
     */
    private static final Duration STARTUP_GRACE = Duration.ofSeconds(30);

    /**
     * How long the process lingers after a decommission finishes. The operator's CLI is blocked
     * in a JMX call whose reply carries the summary; exiting the instant the coordinator returns
     * would drop that reply and the CLI would report a broken connection for a decommission that
     * in fact succeeded.
     */
    private static final Duration DECOMMISSION_EXIT_GRACE = Duration.ofSeconds(2);

    private final NodeConfiguration configuration;
    private final ServiceFactory factory;

    /** Started services, in startup order. Shutdown walks it backwards. */
    private final Map<String, EmbeddedService> started = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, ServiceStatus> lastSeenStatus = new ConcurrentHashMap<>();

    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final AtomicBoolean fatal = new AtomicBoolean();
    private final AtomicInteger exitCode = new AtomicInteger();
    private final CountDownLatch terminated = new CountDownLatch(1);

    private volatile SupervisorState state = SupervisorState.NEW;
    private volatile String failureMessage;
    private volatile Thread shutdownHook;
    private volatile ScheduledExecutorService healthMonitor;
    private volatile DecommissionCoordinator decommissionInFlight;
    private volatile DecommissionProgress decommissionProgress = DecommissionProgress.NONE;
    private volatile boolean mbeanRegistered;

    public Supervisor(NodeConfiguration configuration) {
        this(configuration, ServiceFactory.isolated());
    }

    /** @param factory how each service is instantiated; {@code ServiceFactory.isolated()} in production */
    public Supervisor(NodeConfiguration configuration, ServiceFactory factory) {
        this.configuration = configuration;
        this.factory = factory;
    }

    // --- startup -----------------------------------------------------------------------------

    /**
     * Brings every enabled service up, in order, and leaves the supervisor {@code RUNNING}.
     *
     * <p>If any service fails to start, the ones already up are stopped in reverse order before
     * this throws, so a half-started process never survives the attempt.
     */
    public void start() {
        if (state != SupervisorState.NEW) {
            throw new SupervisorException("start() may only be called once; supervisor is " + state);
        }
        state = SupervisorState.STARTING;
        // Registered before the first service, not after the last: a Ctrl-C during a five-minute
        // Cassandra startup must still unwind whatever has been allocated by then.
        installShutdownHook();
        try {
            for (ServiceConfiguration service : configuration.services().values()) {
                if (!service.enabled()) {
                    LOG.info("Service '{}' is disabled; skipping", service.name());
                    continue;
                }
                startService(service);
            }
        } catch (RuntimeException | Error e) {
            LOG.error("Startup failed: {}", e.toString());
            shutdown(1, e.getMessage());
            throw e;
        }
        registerMBean();
        startHealthMonitor();
        state = SupervisorState.RUNNING;
        LOG.info("cassandra-opensearch is RUNNING; services {}", started.keySet());
    }

    private void startService(ServiceConfiguration service) {
        LOG.info("Starting service '{}'", service.name());
        EmbeddedService instance;
        try {
            instance = factory.create(service);
        } catch (Exception e) {
            throw new SupervisorException(
                    "Cannot create the isolated loader for service '" + service.name()
                            + "' from " + service.libDirectory() + ": " + e.getMessage(), e);
        }
        // Recorded before start() rather than after: a service that fails halfway through
        // startup still holds whatever it managed to allocate, and shutdown has to reach it.
        started.put(service.name(), instance);

        ServiceContext context = new SupervisorServiceContext(
                configuration.homeDirectory(), service, derivedSettings(service.name()), this::onFatalError);
        try {
            TimeLimited.run(service.name() + " startup",
                    service.startupTimeout().plus(STARTUP_GRACE), () -> instance.start(context));
        } catch (Exception e) {
            throw new SupervisorException(
                    "Service '" + service.name() + "' failed to start: " + e.getMessage(), e);
        }
        awaitRunning(service, instance);
        lastSeenStatus.put(service.name(), ServiceStatus.RUNNING);
        LOG.info("Service '{}' is RUNNING: {}", service.name(), instance.details());
    }

    /**
     * Confirms the service reports {@code RUNNING} after its {@code start()} returned.
     *
     * <p>Both runtimes block until they are serving, so this normally passes on the first read.
     * It is not therefore redundant: {@code start()} returning while {@code status()} says
     * anything else means the service and the supervisor disagree about whether it is up, and
     * that disagreement must stop the process rather than be papered over.
     */
    private void awaitRunning(ServiceConfiguration service, EmbeddedService instance) {
        long deadline = System.nanoTime() + service.startupTimeout().toNanos();
        while (true) {
            ServiceStatus status = instance.status();
            if (status == ServiceStatus.RUNNING) {
                return;
            }
            if (status.isTerminal()) {
                throw new SupervisorException("Service '" + service.name() + "' reported " + status
                        + " instead of RUNNING after start() returned: " + instance.details());
            }
            if (System.nanoTime() - deadline > 0) {
                throw new SupervisorException("Service '" + service.name() + "' was still "
                        + status + " " + TimeLimited.format(service.startupTimeout())
                        + " after start() returned");
            }
            sleep(100);
        }
    }

    /**
     * Settings the supervisor can only work out at runtime.
     *
     * <p>OpenSearch's {@code node.name} is taken from the Cassandra node's host id, because that
     * name is what a decommission's {@code cluster.routing.allocation.exclude._name} keys on and
     * it therefore has to be stable across restarts and unique among nodes. A hostname is neither
     * when two nodes share a machine or a container image; Cassandra's host id is persisted in
     * {@code system.local} and is exactly the identity the rest of this node is already known by.
     */
    private Map<String, String> derivedSettings(String serviceName) {
        if (!"opensearch".equals(serviceName)) {
            return Map.of();
        }
        EmbeddedService cassandra = started.get("cassandra");
        if (cassandra == null || !cassandra.status().isServing()) {
            return Map.of();
        }
        String hostId = cassandra.details().get("hostId");
        if (hostId == null || hostId.isBlank()) {
            return Map.of();
        }
        LOG.info("Naming the OpenSearch node after the Cassandra host id: {}", hostId);
        return Map.of("node_name", hostId);
    }

    // --- shutdown ----------------------------------------------------------------------------

    /** Stops every started service in reverse order. Idempotent, and safe to race the hook. */
    public void stop() {
        shutdown(exitCode.get(), null);
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Blocks until the process should exit.
     *
     * @return the exit code {@code main} must use: 0 for an orderly stop or a completed
     *         decommission, non-zero when a service failed or asked for the process to die
     */
    public int awaitShutdown() throws InterruptedException {
        terminated.await();
        return exitCode.get();
    }

    public SupervisorState state() {
        return state;
    }

    /** Why the process is going down, when it is going down unhappily. May be null. */
    public String failureMessage() {
        return failureMessage;
    }

    private void shutdown(int code, String reason) {
        if (!shuttingDown.compareAndSet(false, true)) {
            return;
        }
        exitCode.set(code);
        if (reason != null) {
            failureMessage = reason;
        }
        state = SupervisorState.STOPPING;
        LOG.info("Shutting down{}", reason == null ? "" : ": " + reason);

        DecommissionCoordinator inFlight = decommissionInFlight;
        if (inFlight != null) {
            inFlight.cancel();
        }
        stopHealthMonitor();
        unregisterMBean();
        stopServicesInReverseOrder();

        state = code == 0 ? SupervisorState.STOPPED : SupervisorState.FAILED;
        removeShutdownHook();
        terminated.countDown();
        LOG.info("Shutdown complete; exit code {}", code);
    }

    private void stopServicesInReverseOrder() {
        List<Map.Entry<String, EmbeddedService>> entries;
        synchronized (started) {
            entries = new ArrayList<>(started.entrySet());
        }
        Collections.reverse(entries);
        for (Map.Entry<String, EmbeddedService> entry : entries) {
            stopService(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Stops one service and discards its ClassLoader.
     *
     * <p>A failure is logged and swallowed. The services after this one in the shutdown order are
     * the ones still holding threads, sockets and mapped files, and stranding them because an
     * earlier service threw on the way out is the worse outcome.
     */
    private void stopService(String name, EmbeddedService service) {
        Duration timeout = configuration.service(name).shutdownTimeout();
        LOG.info("Stopping service '{}'", name);
        try {
            TimeLimited.run("stop " + name, timeout, service::stop);
        } catch (Exception e) {
            LOG.warn("Service '{}' did not stop cleanly; continuing with the shutdown", name, e);
        }
        // close() releases the isolated loader, which stop() alone does not. It calls stop()
        // again on the way; the SPI requires that to be a no-op.
        if (service instanceof AutoCloseable closeable) {
            try {
                TimeLimited.run("close " + name, timeout, closeable::close);
            } catch (Exception e) {
                LOG.warn("Could not release the ClassLoader for service '{}'", name, e);
            }
        }
        lastSeenStatus.put(name, service.status());
    }

    private void installShutdownHook() {
        Thread hook = new Thread(() -> shutdown(exitCode.get(), "JVM is shutting down"),
                "cassandra-opensearch-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        shutdownHook = hook;
    }

    private void removeShutdownHook() {
        Thread hook = shutdownHook;
        shutdownHook = null;
        if (hook == null || hook == Thread.currentThread()) {
            return;
        }
        try {
            Runtime.getRuntime().removeShutdownHook(hook);
        } catch (IllegalStateException alreadyExiting) {
            // The JVM is already running its hooks; ours has nothing left to do either way.
        }
    }

    /** Exposed so the shutdown-hook idempotency test can run the hook the JVM would run. */
    Thread shutdownHook() {
        return shutdownHook;
    }

    // --- health ------------------------------------------------------------------------------

    private void startHealthMonitor() {
        Duration interval = configuration.healthCheckInterval();
        healthMonitor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "supervisor-health");
            thread.setDaemon(true);
            return thread;
        });
        healthMonitor.scheduleWithFixedDelay(
                this::checkHealth, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void stopHealthMonitor() {
        ScheduledExecutorService monitor = healthMonitor;
        healthMonitor = null;
        if (monitor != null) {
            monitor.shutdownNow();
        }
    }

    /**
     * Polls each service and logs what moved.
     *
     * <p>Only meaningful while the supervisor is {@code RUNNING}: during a decommission or a
     * shutdown the statuses are supposed to be changing, and reacting to them would race the
     * operation that is deliberately causing them.
     *
     * <p>Nothing here may throw — an exception out of a {@code scheduleWithFixedDelay} task
     * cancels the schedule silently, and health supervision would stop for the life of the
     * process with no indication that it had.
     */
    private void checkHealth() {
        if (state != SupervisorState.RUNNING) {
            return;
        }
        try {
            synchronized (started) {
                started.forEach(this::checkService);
            }
        } catch (RuntimeException e) {
            LOG.warn("Health check failed; will retry", e);
        }
    }

    private void checkService(String name, EmbeddedService service) {
        ServiceStatus current = service.status();
        ServiceStatus previous = lastSeenStatus.put(name, current);
        if (previous != current) {
            LOG.info("Service '{}' moved {} -> {}: {}", name, previous, current, service.details());
        }
        if (current == ServiceStatus.FAILED) {
            onFatalError("service '" + name + "' reported FAILED: " + service.details(), null);
        }
    }

    /**
     * A service has decided the process should die — either through {@code
     * ServiceContext.reportFatalError} or by turning up {@code FAILED} in a health check.
     *
     * <p>The shutdown runs on its own thread because {@code reportFatalError} is called from
     * inside the failing service, frequently from a thread that the ordered shutdown is about to
     * try to join. Doing the work on the caller's thread deadlocks that case.
     */
    private void onFatalError(String message, Throwable cause) {
        if (!fatal.compareAndSet(false, true)) {
            return;
        }
        LOG.error("Fatal error, shutting the process down: {}", message, cause);
        Thread thread = new Thread(() -> shutdown(1, message), "supervisor-fatal-shutdown");
        thread.setDaemon(true);
        thread.start();
    }

    // --- decommission ------------------------------------------------------------------------

    /**
     * Runs the coupled decommission and, on success, schedules the process to exit 0.
     *
     * @param timeout {@code --timeout}; null to use the configured per-phase limits
     */
    public String decommission(Duration timeout, boolean force) throws DecommissionException {
        if (state != SupervisorState.RUNNING) {
            throw new DecommissionException(
                    "Cannot decommission: the supervisor is " + state + ", not RUNNING.");
        }
        Map<String, EmbeddedService> services;
        synchronized (started) {
            services = new LinkedHashMap<>(started);
        }
        DecommissionCoordinator coordinator =
                new DecommissionCoordinator(configuration, services, this::onDecommissionProgress);
        decommissionInFlight = coordinator;
        state = SupervisorState.DECOMMISSIONING;
        try {
            String summary = coordinator.run(timeout, force);
            LOG.info("{}", summary);
            scheduleExitAfterDecommission();
            return summary;
        } catch (DecommissionException e) {
            // Phases 1 and 2 leave the node up and serving, so the supervisor goes back to
            // RUNNING and the operator can retry. A phase-3 failure is reported by the service
            // itself, which knows whether it actually left.
            state = SupervisorState.RUNNING;
            onDecommissionProgress("failed", -1, e.getMessage());
            throw e;
        } finally {
            decommissionInFlight = null;
        }
    }

    private void onDecommissionProgress(String phase, int percentComplete, String message) {
        decommissionProgress = new DecommissionProgress(phase, percentComplete, message);
    }

    public DecommissionProgress decommissionProgress() {
        return decommissionProgress;
    }

    private void scheduleExitAfterDecommission() {
        Thread thread = new Thread(() -> {
            sleep(DECOMMISSION_EXIT_GRACE.toMillis());
            shutdown(0, "decommissioned");
        }, "supervisor-decommission-exit");
        thread.setDaemon(true);
        thread.start();
    }

    // --- observation -------------------------------------------------------------------------

    /** The started services, in startup order. */
    public Map<String, EmbeddedService> services() {
        synchronized (started) {
            return Collections.unmodifiableMap(new LinkedHashMap<>(started));
        }
    }

    public NodeConfiguration configuration() {
        return configuration;
    }

    // --- JMX ---------------------------------------------------------------------------------

    /**
     * Registers the supervisor MBean on the <b>platform</b> MBean server.
     *
     * <p>There is no clash to worry about: the Cassandra runtime keeps {@code
     * org.apache.cassandra:*} in a private MBean server precisely so the platform namespace stays
     * free, and OpenSearch registers nothing when it is not booted through its own {@code
     * Bootstrap}.
     *
     * <p>A registration failure is logged rather than fatal. It costs the operator {@code
     * bin/cassandra-opensearch status} and the supervisor-driven decommission, which is bad — but
     * not as bad as refusing to run a node whose two servers are both healthy.
     */
    private void registerMBean() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            server.registerMBean(new SupervisorControl(this), new ObjectName(MBEAN_NAME));
            mbeanRegistered = true;
            LOG.info("Registered {} on the platform MBean server", MBEAN_NAME);
        } catch (Exception e) {
            LOG.error("Could not register {}; 'bin/cassandra-opensearch status' and the"
                    + " supervisor-driven decommission will not be able to reach this process",
                    MBEAN_NAME, e);
        }
    }

    private void unregisterMBean() {
        if (!mbeanRegistered) {
            return;
        }
        mbeanRegistered = false;
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(new ObjectName(MBEAN_NAME));
        } catch (Exception e) {
            LOG.warn("Could not unregister {}", MBEAN_NAME, e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SupervisorException("Interrupted while waiting for a service", e);
        }
    }
}
