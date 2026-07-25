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
import io.cassandraopensearch.spi.DecommissionContext;
import io.cassandraopensearch.spi.EmbeddedService;
import io.cassandraopensearch.spi.RingStateEvent;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
 * never register theirs, and this one hook runs the ordered shutdown instead.
 *
 * <p>{@link #stop()} is idempotent with it, but idempotent here means "does not do the work
 * twice", <b>not</b> "returns immediately". Whichever call gets there first does the work and the
 * other <i>waits for it</i>. That distinction is the whole contract: {@code Shutdown.runHooks()}
 * halts the JVM as soon as every hook has returned, so a hook that returned early because a
 * fatal-error thread was already three minutes into {@code drain()} would halt the JVM on top of
 * a half-drained Cassandra — which then replays its commit log on the next start, if it can. The
 * wait is bounded by {@link #shutdownBudget()}; past that the process is stuck either way and
 * saying so is better than hanging forever.
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

    /**
     * How long a shutdown waits for an in-flight decommission to unwind after cancelling it.
     *
     * <p>Not the decommission's own timeouts, which run to hours: cancellation is observed at the
     * next poll and every phase after it is separately bounded by {@link TimeLimited}, so what is
     * actually being waited for is one poll interval plus the compensating {@code
     * abortDecommission} calls. A minute is generous for that and short enough that a SIGTERM
     * still produces a process that goes away.
     */
    private static final Duration DECOMMISSION_CANCEL_BUDGET = Duration.ofMinutes(1);

    /** Slack on top of the per-service limits, for the bookkeeping either side of them. */
    private static final Duration SHUTDOWN_BUDGET_SLACK = Duration.ofSeconds(30);

    private final NodeConfiguration configuration;
    private final ServiceFactory factory;

    /** Started services, in startup order. Shutdown walks it backwards. */
    private final Map<String, EmbeddedService> started = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, ServiceStatus> lastSeenStatus = new ConcurrentHashMap<>();

    /**
     * Services whose {@code start()} overran its deadline and is still running on a thread nobody
     * can stop. The shutdown walk skips them: see {@link #stopServicesInReverseOrder}.
     */
    private final Set<String> abandoned = new CopyOnWriteArraySet<>();

    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final AtomicBoolean fatal = new AtomicBoolean();
    private final AtomicBoolean externalDecommissionSeen = new AtomicBoolean();
    private final AtomicInteger exitCode = new AtomicInteger();
    private final CountDownLatch terminated = new CountDownLatch(1);

    /**
     * Guards the two transitions that must not interleave: RUNNING to DECOMMISSIONING, and
     * anything to STOPPING. Both read the state and the in-flight coordinator and then write
     * both, and doing that unguarded is how two JMX callers ran two coordinators over the same
     * pair of services.
     */
    private final Object transitionLock = new Object();

    private final AtomicReference<DecommissionCoordinator> decommissionInFlight = new AtomicReference<>();

    private volatile SupervisorState state = SupervisorState.NEW;
    private volatile String failureMessage;
    private volatile Thread shutdownHook;
    private volatile ScheduledExecutorService healthMonitor;
    private volatile DecommissionProgress decommissionProgress = DecommissionProgress.NONE;
    private volatile boolean mbeanRegistered;

    /**
     * {@link #STARTUP_GRACE}, overridable from a test that has to provoke an abandoned {@code
     * start()} and cannot spend thirty seconds doing it.
     */
    private volatile Duration startupGrace = STARTUP_GRACE;

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
            // Inside the try, not after it. These two can fail on their own — an invalid health
            // check interval is enough — and outside the try that failure propagated with both
            // services left running and nothing to stop them.
            registerMBean();
            startHealthMonitor();
        } catch (RuntimeException | Error e) {
            LOG.error("Startup failed: {}", e.toString());
            shutdown(1, e.getMessage() == null ? e.toString() : e.getMessage());
            throw e;
        }
        state = SupervisorState.RUNNING;
        LOG.info("cassandra-opensearch is RUNNING; services {}", started.keySet());
    }

    /** Test seam for {@link #STARTUP_GRACE}; see the field. */
    void startupGrace(Duration grace) {
        this.startupGrace = grace;
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
                configuration.homeDirectory(), service, derivedSettings(service.name()),
                this::onServiceEvent, this::onFatalError);
        try {
            TimeLimited.run(service.name() + " startup",
                    service.startupTimeout().plus(startupGrace), () -> instance.start(context));
        } catch (TimeLimited.AbandonedException e) {
            abandon(service.name(), e);
            throw new SupervisorException(
                    "Service '" + service.name() + "' failed to start: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new SupervisorException(
                    "Service '" + service.name() + "' failed to start: " + e.getMessage(), e);
        }
        awaitRunning(service, instance);
        lastSeenStatus.put(service.name(), ServiceStatus.RUNNING);
        LOG.info("Service '{}' is RUNNING: {}", service.name(), instance.details());
    }

    /**
     * Records that a service's {@code start()} overran its deadline and is still running.
     *
     * <p>The unwind that follows must not touch it. {@code stop()} on such a service latches — it
     * is a CAS in both runtimes — so an early {@code stop()} can win the latch while the abandoned
     * {@code start()} is still inside code that ignores interrupts, and the late thread then
     * finishes, publishes {@code RUNNING} and installs its background threads on top of a
     * supervisor that has already declared the service stopped. Those threads can never be
     * stopped afterwards, because {@code stop()} has already been used up.
     *
     * <p>Closing its ClassLoader would be worse still: the abandoned thread is executing classes
     * out of it.
     */
    private void abandon(String serviceName, TimeLimited.AbandonedException cause) {
        abandoned.add(serviceName);
        LOG.error("Service '{}' did not finish starting and has been abandoned: {}."
                        + " It is still running on a thread this process cannot stop, so it will"
                        + " NOT be stopped and its ClassLoader will NOT be released during the"
                        + " shutdown that follows — doing either would race the abandoned startup"
                        + " and could leave threads behind that nothing can stop. This JVM cannot"
                        + " shut down cleanly: kill it (SIGKILL) once the other services are down,"
                        + " and expect '{}' to recover on the next start as it would from a crash.",
                serviceName, cause.getMessage(), serviceName);
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

    /**
     * Stops every started service in reverse order and returns once they are all down.
     *
     * <p>Idempotent, and safe to race the shutdown hook — but idempotent in the sense that the
     * work happens once, not in the sense that a second caller returns early. A caller that loses
     * the race waits for the shutdown that is already running, up to {@link #shutdownBudget()},
     * because this method's whole promise is that the services are stopped when it returns.
     */
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
            // Not our shutdown to run — but the reason we were called still has to be recorded,
            // and above all we must not return until the shutdown that is running has finished.
            escalate(code, reason);
            awaitShutdownInFlight(reason);
            return;
        }
        escalate(code, reason);
        DecommissionCoordinator inFlight;
        synchronized (transitionLock) {
            state = SupervisorState.STOPPING;
            inFlight = decommissionInFlight.get();
        }
        LOG.info("Shutting down{}", reason == null ? "" : ": " + reason);

        // Before anything is stopped. The coordinator drives the same two services this is about
        // to stop and close the loaders of, and it does not read `cancelled` on its own — so
        // cancelling without waiting used to let it call decommission(cassandra), streaming
        // ranges through jars whose ClassLoader had already been closed.
        if (inFlight != null) {
            cancelDecommission(inFlight);
        }
        stopHealthMonitor();
        unregisterMBean();
        stopServicesInReverseOrder();

        // Re-read rather than trusting the local: a service may have reported a fatal error while
        // this shutdown was in flight, and that has to reach the exit code and the state.
        int finalCode = exitCode.get();
        state = finalCode == 0 ? SupervisorState.STOPPED : SupervisorState.FAILED;
        removeShutdownHook();
        terminated.countDown();
        LOG.info("Shutdown complete; exit code {}", finalCode);
    }

    /**
     * Records why the process is going down, whether or not this caller runs the shutdown.
     *
     * <p>A fatal error that arrives while an orderly shutdown is already in flight is still a
     * fatal error: dropping it left the process exiting 0 with a null failure message, telling the
     * operator and their supervisor daemon that a node which had just lost its commit log
     * directory had stopped on request.
     */
    private void escalate(int code, String reason) {
        if (code == 0) {
            return;
        }
        exitCode.compareAndSet(0, code);
        if (reason != null && failureMessage == null) {
            failureMessage = reason;
        }
    }

    /** Cancels an in-flight decommission and waits for it to let go of the services. */
    private void cancelDecommission(DecommissionCoordinator inFlight) {
        LOG.warn("A decommission is in flight; cancelling it and waiting up to {} for it to unwind"
                + " before any service is stopped", TimeLimited.format(DECOMMISSION_CANCEL_BUDGET));
        inFlight.cancel();
        if (inFlight.awaitCompletion(DECOMMISSION_CANCEL_BUDGET)) {
            LOG.info("The decommission has unwound; continuing with the shutdown");
            return;
        }
        LOG.error("The decommission did not unwind within {}. Continuing with the shutdown anyway,"
                        + " but the coordinator is still driving these services: check `nodetool"
                        + " netstats` and the OpenSearch allocation exclusions on this cluster"
                        + " before treating this node as cleanly stopped.",
                TimeLimited.format(DECOMMISSION_CANCEL_BUDGET));
    }

    /**
     * Waits for the shutdown another thread is running, because returning early is not an option.
     *
     * <p>The caller here is usually the JVM's shutdown hook, and {@code Shutdown.runHooks()} halts
     * the JVM the moment every hook has returned. Returning while another thread is inside
     * Cassandra's {@code drain()} halts the JVM in the middle of it.
     */
    private void awaitShutdownInFlight(String reason) {
        Duration budget = shutdownBudget();
        LOG.info("A shutdown is already in flight{}; waiting up to {} for it to complete",
                reason == null ? "" : " (" + reason + ")", TimeLimited.format(budget));
        try {
            if (terminated.await(budget.toMillis(), TimeUnit.MILLISECONDS)) {
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for the shutdown already in flight");
            return;
        }
        LOG.error("The shutdown already in flight has not completed within {}. Whatever happens"
                        + " next — including the JVM halting — happens on top of it; a service"
                        + " that was mid-drain may have to recover on the next start.",
                TimeLimited.format(budget));
    }

    /**
     * The longest a shutdown can legitimately take: every service's own limit, twice over for the
     * {@code stop()} and the {@code close()} it gets, plus the decommission cancellation that may
     * precede them and a little slack for the bookkeeping around both.
     */
    private Duration shutdownBudget() {
        Duration total = DECOMMISSION_CANCEL_BUDGET.plus(SHUTDOWN_BUDGET_SLACK);
        for (ServiceConfiguration service : configuration.services().values()) {
            total = total.plus(service.shutdownTimeout().multipliedBy(2));
        }
        return total;
    }

    private void stopServicesInReverseOrder() {
        List<Map.Entry<String, EmbeddedService>> entries;
        synchronized (started) {
            entries = new ArrayList<>(started.entrySet());
        }
        Collections.reverse(entries);
        for (Map.Entry<String, EmbeddedService> entry : entries) {
            if (abandoned.contains(entry.getKey())) {
                LOG.error("Not stopping service '{}': its start() was abandoned and is still"
                                + " running. Stopping it now would race that thread for the one"
                                + " stop() it gets. This JVM will not exit on its own; kill it.",
                        entry.getKey());
                continue;
            }
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
     * process with no indication that it had. {@code Throwable}, not {@code RuntimeException}:
     * these calls cross into an isolated ClassLoader, where {@code NoClassDefFoundError} and
     * {@code LinkageError} are the errors that actually turn up, and an {@code Error} escaping
     * here would cancel the schedule just as permanently and just as silently.
     */
    private void checkHealth() {
        if (state != SupervisorState.RUNNING) {
            return;
        }
        try {
            synchronized (started) {
                started.forEach(this::checkService);
            }
        } catch (Throwable e) {
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
        // Recorded here rather than only inside the shutdown, and on the caller's thread rather
        // than the one below: if a shutdown is already in flight this call will lose its CAS, and
        // the process would otherwise exit 0 with no failure message for a node that has just
        // told us it is broken.
        escalate(1, message);
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
        DecommissionCoordinator coordinator = beginDecommission();
        try {
            String summary = coordinator.run(timeout, force);
            LOG.info("{}", summary);
            scheduleExitAfterDecommission();
            return summary;
        } catch (DecommissionException e) {
            // Phases 1 and 2 leave the node up and serving, so the supervisor goes back to
            // RUNNING and the operator can retry. A phase-3 failure is reported by the service
            // itself, which knows whether it actually left.
            synchronized (transitionLock) {
                // Only if this decommission is still what the supervisor is doing. A shutdown may
                // be what cancelled it, and putting the state back to RUNNING underneath a
                // shutdown would readmit a second decommission into a process that is leaving.
                if (state == SupervisorState.DECOMMISSIONING) {
                    state = SupervisorState.RUNNING;
                }
            }
            onDecommissionProgress("failed", -1, e.getMessage());
            throw e;
        } finally {
            decommissionInFlight.compareAndSet(coordinator, null);
        }
    }

    /**
     * Claims the right to decommission, atomically.
     *
     * <p>Reading the state and then installing a coordinator is a check-then-act, and this is a
     * JMX operation: the call blocks for as long as the ring takes to hand over, printing nothing,
     * which is exactly the situation in which an operator opens a second terminal and runs it
     * again. Both callers saw {@code RUNNING}, both got a coordinator, and the SPI's "never
     * concurrently" contract was broken with two {@code StorageService.decommission()} calls in
     * flight over one node.
     *
     * @throws DecommissionException if a decommission is already running, or the supervisor is in
     *                               no state to start one
     */
    private DecommissionCoordinator beginDecommission() throws DecommissionException {
        synchronized (transitionLock) {
            DecommissionCoordinator existing = decommissionInFlight.get();
            if (existing != null) {
                throw new DecommissionException(
                        "Cannot decommission: a decommission is already in flight on this node."
                                + " It reports no output while it waits, which is normal — watch"
                                + " it with `bin/cassandra-opensearch status`. Running a second"
                                + " one would drive both services twice over.");
            }
            if (state != SupervisorState.RUNNING) {
                throw new DecommissionException(
                        "Cannot decommission: the supervisor is " + state + ", not RUNNING.");
            }
            Map<String, EmbeddedService> services;
            synchronized (started) {
                services = new LinkedHashMap<>(started);
            }
            DecommissionCoordinator coordinator = new DecommissionCoordinator(
                    configuration, services, this::onDecommissionProgress);
            decommissionInFlight.set(coordinator);
            state = SupervisorState.DECOMMISSIONING;
            return coordinator;
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

    // --- decommission started outside the supervisor ------------------------------------------

    /**
     * Every event a service reports, on the service's own thread.
     *
     * <p>Must stay cheap: the SPI promises {@code reportEvent} neither blocks nor throws, and the
     * caller is frequently a Cassandra thread in the middle of something. Everything here is a
     * parse and a few volatile reads; the one thing that can wait is handed to a thread of our
     * own in {@link #onNodeLeavingTheRing}.
     */
    private void onServiceEvent(String serviceName, String level, String type, String message) {
        if (!RingStateEvent.TYPE.equals(type)) {
            return;
        }
        RingStateEvent event = RingStateEvent.parse(message);
        if (event != null && event.leaving()) {
            onNodeLeavingTheRing(serviceName, event);
        }
    }

    /**
     * A service has reported that this node is leaving its cluster. Works out whether the
     * supervisor asked for that, and reacts only if it did not.
     *
     * <h2>Telling the two apart</h2>
     *
     * The supported path drives Cassandra into exactly the same operation modes, so the event
     * alone cannot distinguish {@code bin/cassandra-opensearch decommission} from {@code
     * bin/nodetool decommission}. The supervisor's own state can: a coordinated decommission is
     * always {@link SupervisorState#DECOMMISSIONING} (or, for the instant between the coordinator
     * being installed and the state moving, has a coordinator in flight) by the time Cassandra has
     * begun to leave. Reacting during that window would run a second, uncoordinated exclusion
     * against the OpenSearch node the coordinator is already driving.
     *
     * <h2>What this can and cannot achieve</h2>
     *
     * It reduces harm; it does not make the unsupervised path safe. By the time Cassandra reports
     * {@code LEAVING} the ring transition has already begun, and nothing here can put that back or
     * slow it down. All the supervisor can still do is start OpenSearch's shard relocation — which
     * the supported path starts <i>before</i> Cassandra is touched at all — and hope it finishes
     * first. On a node holding a lot of index data it will not. The exclusion is a catch-up that
     * may lose the race, and the operator is told so.
     */
    private void onNodeLeavingTheRing(String serviceName, RingStateEvent event) {
        if (!configuration.decommission().watchExternalDecommission()) {
            return;
        }
        if (decommissionInFlight.get() != null || state != SupervisorState.RUNNING) {
            LOG.debug("Service '{}' reports {}; the supervisor is {} and is driving this itself",
                    serviceName, event.state(), state);
            return;
        }
        if (!externalDecommissionSeen.compareAndSet(false, true)) {
            return;
        }
        // Off the reporting thread: the exclusion is a round trip into OpenSearch's cluster state,
        // and the caller is a service thread that the SPI forbids us to block.
        Thread thread = new Thread(() -> excludeAfterExternalDecommission(serviceName, event),
                "supervisor-external-decommission");
        thread.setDaemon(true);
        thread.start();
    }

    private void excludeAfterExternalDecommission(String serviceName, RingStateEvent event) {
        // The guard that let this thread start ran on the reporting thread; between that decision
        // and this line a supervisor-driven decommission or a shutdown may have begun, and either
        // one is already driving the OpenSearch node this method is about to touch.
        if (decommissionInFlight.get() != null || state != SupervisorState.RUNNING) {
            LOG.debug("Service '{}' reported {}, but the supervisor moved to {} before the"
                            + " catch-up exclusion could start; leaving it to whatever is driving"
                            + " this now", serviceName, event.state(), state);
            return;
        }
        EmbeddedService opensearch = started.get("opensearch");
        LOG.warn("Service '{}' is {}: Cassandra is leaving the ring but was not asked through the"
                        + " supervisor; OpenSearch shard relocation may not complete before this"
                        + " process exits — use `bin/cassandra-opensearch decommission`, which"
                        + " relocates the shards before Cassandra gives its ranges back.",
                serviceName, event.state());
        if (opensearch == null) {
            return;
        }
        LOG.warn("Excluding this node from OpenSearch shard allocation as a best-effort catch-up."
                + " It is a catch-up and it may lose the race: watch `_cat/shards` and let this"
                + " node empty before the process is stopped, or any shard with no other copy is"
                + " lost with it.");
        Duration timeout = configuration.decommission().shardRelocationTimeout();
        try {
            opensearch.prepareDecommission(new ExternalCatchUp(timeout));
        } catch (Exception e) {
            LOG.error("Could not exclude this node from OpenSearch shard allocation after a"
                    + " decommission started outside the supervisor. Its shards will not relocate"
                    + " on their own; exclude the node by hand with a transient"
                    + " cluster.routing.allocation.exclude._name update.", e);
        }
    }

    /**
     * The phase context for the catch-up exclusion.
     *
     * <p>Never forced and never cancelled, because there is nothing here to force or to cancel:
     * this makes one call, {@code prepareDecommission}, which neither waits for a hand-off nor
     * decides whether data may be abandoned. The decision that {@code force} would answer was
     * taken by whoever ran {@code nodetool decommission}.
     */
    private record ExternalCatchUp(Duration timeout) implements DecommissionContext {

        @Override
        public boolean force() {
            return false;
        }

        @Override
        public void reportProgress(int percentComplete, String message) {
            LOG.info("[external-decommission] {}", message);
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
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
