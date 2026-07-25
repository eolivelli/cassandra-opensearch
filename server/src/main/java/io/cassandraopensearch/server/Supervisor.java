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
     * How long a shutdown waits for an in-flight decommission — the coordinated one, or the
     * catch-up exclusion of {@link #onNodeLeavingTheRing} — to let go of the services after it has
     * been cancelled.
     *
     * <p>Cancellation is cooperative, and it is <b>not</b> observed everywhere. {@code
     * awaitDecommissionReady} polls {@link DecommissionContext#isCancelled()}, and {@link
     * DecommissionCoordinator} checks at every phase boundary — but a phase that has already begun
     * never looks again, and the longest of them is past the last boundary: {@code
     * decommission-cassandra} is bounded by {@code ring_streaming_timeout} plus a grace, an hour
     * and a half by default, and {@code StorageService.decommission()} neither polls a cancellation
     * flag nor honours an interrupt. A shutdown that arrives there will spend this whole budget and
     * still find the coordinator running.
     *
     * <p>So this is a real deadline rather than a formality, and what happens when it expires is
     * the point: those services are treated as {@link #abandon abandoned} — not stopped, not
     * closed, exit code non-zero. Stopping a service whose {@code decommission()} is still
     * streaming ranges takes the one {@code stop()} it will ever honour, and closing its
     * ClassLoader pulls the jars out from under the thread that is executing them.
     *
     * <p>A minute is generous for the cases cancellation <i>is</i> observed in, and short enough
     * that a SIGTERM still produces a process that goes away.
     */
    private static final Duration DECOMMISSION_CANCEL_BUDGET = Duration.ofMinutes(1);

    /**
     * Added to {@code shard_relocation_timeout} when bounding the catch-up exclusion from outside,
     * for the same reason as {@link #STARTUP_GRACE}: the service's own message is the useful one.
     */
    private static final Duration EXTERNAL_EXCLUSION_GRACE = Duration.ofSeconds(30);

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

    /**
     * The catch-up exclusion started by {@link #onNodeLeavingTheRing}, while it is running.
     *
     * <p>Registered under {@link #transitionLock} by the thread that runs it, so a shutdown either
     * sees it and waits for it or does not, in which case the exclusion sees {@code STOPPING} and
     * never makes its call. Unregistered means "no thread of ours is inside OpenSearch"; anything
     * weaker is a check-then-act, and the act is {@code stop()} plus {@code close()} on the loader
     * that thread is executing out of.
     */
    private final AtomicReference<ExternalCatchUp> externalExclusion = new AtomicReference<>();

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

    /**
     * {@link #DECOMMISSION_CANCEL_BUDGET}, overridable for the same reason: the tests that pin
     * what happens when it expires would otherwise each take a minute.
     */
    private volatile Duration decommissionCancelBudget = DECOMMISSION_CANCEL_BUDGET;

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
        LOG.info("cassandra-opensearch is RUNNING; services {}", startedServices().keySet());
    }

    /** Test seam for {@link #STARTUP_GRACE}; see the field. */
    void startupGrace(Duration grace) {
        this.startupGrace = grace;
    }

    /** Test seam for {@link #DECOMMISSION_CANCEL_BUDGET}; see the field. */
    void decommissionCancelBudget(Duration budget) {
        this.decommissionCancelBudget = budget;
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
            abandon(service.name(), "it did not finish starting: " + e.getMessage());
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
     * Records that a call into a service overran its deadline and is still running.
     *
     * <p>The unwind that follows must not touch it. {@code stop()} on such a service latches — it
     * is a CAS in both runtimes — so an early {@code stop()} can win the latch while the abandoned
     * call is still inside code that ignores interrupts, and the late thread then finishes,
     * publishes {@code RUNNING} and installs its background threads on top of a supervisor that has
     * already declared the service stopped. Those threads can never be stopped afterwards, because
     * {@code stop()} has already been used up.
     *
     * <p>Closing its ClassLoader would be worse still: the abandoned thread is executing classes
     * out of it.
     *
     * <p>Every caller is a place where a bounded call came back without its work having finished:
     * an overrun {@code start()}, an overrun {@code stop()} or {@code close()}, a decommission that
     * did not unwind inside {@link #DECOMMISSION_CANCEL_BUDGET}. All of them mean the same thing to
     * the shutdown walk — see {@link #stopServicesInReverseOrder} — and all of them mean this JVM
     * cannot exit cleanly, so all of them force a non-zero exit code.
     *
     * @param what what is still running, phrased to follow the service name in a sentence
     */
    private void abandon(String serviceName, String what) {
        abandoned.add(serviceName);
        escalate(1, "service '" + serviceName + "' was abandoned: " + what);
        LOG.error("Service '{}' has been abandoned — {}."
                        + " It is still running on a thread this process cannot stop, so it will"
                        + " NOT be stopped and its ClassLoader will NOT be released during the"
                        + " shutdown that follows — doing either would race that thread and could"
                        + " leave threads behind that nothing can stop. This JVM cannot"
                        + " shut down cleanly: kill it (SIGKILL) once the other services are down,"
                        + " and expect '{}' to recover on the next start as it would from a crash.",
                serviceName, what, serviceName);
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

    /**
     * The one shutdown, run by whichever caller wins the CAS.
     *
     * <p>Everything from the CAS onwards is inside a {@code try}, and the {@code finally} does the
     * two things nothing else can do afterwards: remove the hook and count {@link #terminated}
     * down. The straight-line version of this method had them on the success path, which turned
     * any throw — a {@code NoClassDefFoundError} out of a service whose loader is on its way out,
     * an {@code OutOfMemoryError} from {@link TimeLimited}'s per-call thread, a {@code
     * ConfigurationException} while looking up a timeout — into a process that never exits and
     * cannot be made to: the latch stays closed, so {@code main} blocks forever and every other
     * caller of {@link #stop()} blocks with it, while the CAS is already {@code true} so no one
     * can retry the shutdown that failed.
     */
    private void shutdown(int code, String reason) {
        if (!shuttingDown.compareAndSet(false, true)) {
            // Not our shutdown to run — but the reason we were called still has to be recorded,
            // and above all we must not return until the shutdown that is running has finished.
            escalate(code, reason);
            awaitShutdownInFlight(reason);
            return;
        }
        try {
            escalate(code, reason);
            runShutdown(reason);
        } catch (Throwable t) {
            // Not rethrown: the callers are a shutdown hook, a fatal-error thread and stop(), and
            // none of them can do anything with it that the exit code does not already say. What
            // matters is that the process can still leave, and that it does not claim it left
            // cleanly.
            escalate(1, "the shutdown itself failed: " + t);
            state = SupervisorState.FAILED;
            LOG.error("The shutdown did not run to completion. Services may still be holding"
                    + " threads, sockets and mapped files; the process will exit non-zero and"
                    + " whatever is left has to recover on the next start as it would from a"
                    + " crash.", t);
        } finally {
            removeShutdownHook();
            terminated.countDown();
            LOG.info("Shutdown complete; exit code {}", exitCode.get());
        }
    }

    private void runShutdown(String reason) {
        DecommissionCoordinator inFlight;
        ExternalCatchUp exclusion;
        synchronized (transitionLock) {
            state = SupervisorState.STOPPING;
            inFlight = decommissionInFlight.get();
            exclusion = externalExclusion.get();
        }
        LOG.info("Shutting down{}", reason == null ? "" : ": " + reason);

        // Before anything is stopped. Both of these drive the same services this is about to stop
        // and close the loaders of, and neither reads `cancelled` on its own — so cancelling
        // without waiting used to let the coordinator call decommission(cassandra), streaming
        // ranges through jars whose ClassLoader had already been closed.
        if (inFlight != null) {
            cancelDecommission(inFlight);
        }
        if (exclusion != null) {
            cancelExternalExclusion(exclusion);
        }
        stopHealthMonitor();
        unregisterMBean();
        stopServicesInReverseOrder();

        // Re-read rather than trusting the local: a service may have reported a fatal error while
        // this shutdown was in flight, and that has to reach the exit code and the state.
        state = exitCode.get() == 0 ? SupervisorState.STOPPED : SupervisorState.FAILED;
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

    /**
     * Cancels an in-flight decommission and waits for it to let go of the services.
     *
     * <p>When it does not let go, the services it is driving are abandoned rather than stopped.
     * The phase that cannot be cancelled is {@code decommission-cassandra} — see {@link
     * #DECOMMISSION_CANCEL_BUDGET} — and it is the phase in which stopping and closing anyway does
     * the most damage: the coordinator is inside {@code StorageService.decommission()}, streaming
     * ranges, and {@code close()} would take the {@code URLClassLoader} out from under it. The
     * coordinator stops both services itself when it finishes, which is the only stop they should
     * get.
     */
    private void cancelDecommission(DecommissionCoordinator inFlight) {
        Duration budget = decommissionCancelBudget;
        LOG.warn("A decommission is in flight; cancelling it and waiting up to {} for it to unwind"
                + " before any service is stopped", TimeLimited.format(budget));
        inFlight.cancel();
        if (inFlight.awaitCompletion(budget)) {
            LOG.info("The decommission has unwound; continuing with the shutdown");
            return;
        }
        LOG.error("The decommission did not unwind within {}: the coordinator is still driving"
                        + " {}. Those services will NOT be stopped and their ClassLoaders will NOT"
                        + " be released — the coordinator stops them itself when it finishes, and"
                        + " a decommission phase that is still streaming must not have its jars"
                        + " closed underneath it. Check `nodetool netstats` and the OpenSearch"
                        + " allocation exclusions on this cluster before treating this node as"
                        + " cleanly stopped.",
                TimeLimited.format(budget), inFlight.serviceNames());
        for (String serviceName : inFlight.serviceNames()) {
            abandon(serviceName, "a decommission was still driving it " + TimeLimited.format(budget)
                    + " after being cancelled");
        }
    }

    /**
     * Cancels the catch-up exclusion and waits for it, for the same reasons and with the same
     * outcome: a call still inside OpenSearch when the budget expires means OpenSearch is
     * abandoned, not stopped and closed around it.
     */
    private void cancelExternalExclusion(ExternalCatchUp exclusion) {
        Duration budget = decommissionCancelBudget;
        LOG.warn("A catch-up OpenSearch exclusion is in flight; cancelling it and waiting up to {}"
                + " for it to return before any service is stopped", TimeLimited.format(budget));
        exclusion.cancel();
        if (exclusion.awaitCompletion(budget)) {
            LOG.info("The catch-up exclusion has returned; continuing with the shutdown");
            return;
        }
        abandon("opensearch", "a catch-up shard-allocation exclusion was still inside it "
                + TimeLimited.format(budget) + " after being cancelled");
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
        long deadline = System.nanoTime() + budget.toNanos();
        boolean interrupted = false;
        try {
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                try {
                    if (terminated.await(remaining, TimeUnit.NANOSECONDS)) {
                        return;
                    }
                } catch (InterruptedException e) {
                    // Remembered and put back at the end, but not obeyed: returning here is the
                    // one thing this method exists to prevent. The caller is usually the JVM's
                    // shutdown hook, an interrupt during shutdown is ordinary — every executor
                    // being torn down sends some — and returning early halts the JVM on top of a
                    // service that is still draining.
                    interrupted = true;
                    LOG.warn("Interrupted while waiting for the shutdown already in flight;"
                            + " still waiting, because returning now would let the JVM halt on a"
                            + " live shutdown");
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.error("The shutdown already in flight has not completed within {}. Whatever happens"
                        + " next — including the JVM halting — happens on top of it; a service"
                        + " that was mid-drain may have to recover on the next start.",
                TimeLimited.format(budget));
    }

    /**
     * The longest a shutdown can legitimately take: every service's own limit, twice over for the
     * {@code stop()} and the {@code close()} it gets, plus the cancellations that may precede them
     * — a coordinated decommission and a catch-up exclusion can both be in flight, and each is
     * waited for in turn — and a little slack for the bookkeeping around all of it.
     */
    private Duration shutdownBudget() {
        Duration total = decommissionCancelBudget.multipliedBy(2).plus(SHUTDOWN_BUDGET_SLACK);
        for (ServiceConfiguration service : configuration.services().values()) {
            total = total.plus(service.shutdownTimeout().multipliedBy(2));
        }
        return total;
    }

    /**
     * The started services, copied.
     *
     * <p>Copied, and never held: {@code started} is a {@code synchronizedMap}, so every traversal
     * of it — {@code forEach} included — holds its monitor for the whole traversal. Holding that
     * monitor across a call into an isolated loader is what wedged the process: the health monitor
     * sat in {@code status()} inside a runtime that ignores interrupts, a decommission blocked
     * behind it while holding {@link #transitionLock}, and the shutdown blocked on {@code
     * transitionLock} before it ever reached {@link #stopHealthMonitor}. Nothing here may be
     * called with {@code transitionLock} held.
     */
    private Map<String, EmbeddedService> startedServices() {
        synchronized (started) {
            return new LinkedHashMap<>(started);
        }
    }

    private void stopServicesInReverseOrder() {
        List<Map.Entry<String, EmbeddedService>> entries =
                new ArrayList<>(startedServices().entrySet());
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
     * <p>A failure is logged and swallowed — {@code Throwable}, because these calls cross into an
     * isolated ClassLoader where {@code NoClassDefFoundError} and {@code LinkageError} are what
     * actually turn up. The services after this one in the shutdown order are the ones still
     * holding threads, sockets and mapped files, and stranding them because an earlier service
     * threw on the way out is the worse outcome.
     *
     * <p>An {@link TimeLimited.AbandonedException} is the exception to all of that, and it is
     * caught first. It does not mean "{@code stop()} failed"; it means {@code stop()} is
     * <i>still running</i> inside the service, and the two things this method does next — {@code
     * close()}, which closes the {@code URLClassLoader} that thread is executing out of, and a
     * final {@code status()} that would be recorded as if the service had settled — are precisely
     * what must not happen to it. Handling it as an ordinary failure produced a shutdown that
     * closed a loader under a live {@code drain()} and then reported exit code 0.
     */
    private void stopService(String name, EmbeddedService service) {
        LOG.info("Stopping service '{}'", name);
        Duration timeout;
        try {
            timeout = configuration.service(name).shutdownTimeout();
        } catch (RuntimeException e) {
            LOG.warn("No shutdown timeout is configured for service '{}'; using the default", name, e);
            timeout = ServiceConfiguration.DEFAULT_SHUTDOWN_TIMEOUT;
        }
        try {
            TimeLimited.run("stop " + name, timeout, service::stop);
        } catch (TimeLimited.AbandonedException e) {
            abandon(name, "its stop() is still running: " + e.getMessage());
            return;
        } catch (Throwable t) {
            LOG.warn("Service '{}' did not stop cleanly; continuing with the shutdown", name, t);
        }
        // close() releases the isolated loader, which stop() alone does not. It calls stop()
        // again on the way; the SPI requires that to be a no-op.
        if (service instanceof AutoCloseable closeable) {
            try {
                TimeLimited.run("close " + name, timeout, closeable::close);
            } catch (TimeLimited.AbandonedException e) {
                abandon(name, "its close() is still running: " + e.getMessage());
                return;
            } catch (Throwable t) {
                LOG.warn("Could not release the ClassLoader for service '{}'", name, t);
            }
        }
        recordFinalStatus(name, service);
    }

    /**
     * Records what the service said on the way out, defensively.
     *
     * <p>This is the supervisor's own bookkeeping and it runs <i>after</i> {@code close()}, which
     * is to say after the loader that answers the call has been discarded. {@code
     * IsolatedService.status()} is hardened against exactly that, but the invariant is this
     * class's: an unguarded call here delegates the whole shutdown's ability to finish to a
     * service implementation, and a {@code null} return alone was enough to abort it with an NPE
     * out of {@code ConcurrentHashMap.put}.
     */
    private void recordFinalStatus(String name, EmbeddedService service) {
        try {
            ServiceStatus finalStatus = service.status();
            if (finalStatus != null) {
                lastSeenStatus.put(name, finalStatus);
            } else {
                LOG.warn("Service '{}' reported a null status after being stopped", name);
            }
        } catch (Throwable t) {
            LOG.warn("Could not read the final status of service '{}'", name, t);
        }
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
     *
     * <p>Nothing here may hold a lock either. This polls both services on a fixed schedule, and
     * {@code status()} on a runtime that is wedged does not return — it is not interruptible, so
     * {@link #stopHealthMonitor} cannot free it. Holding the {@code started} monitor across the
     * poll therefore blocked {@link #beginDecommission}, which held {@link #transitionLock} while
     * it waited, which blocked the shutdown before it reached the code that would have stopped
     * this monitor. Three threads, no deadline, and the process never went down.
     */
    private void checkHealth() {
        if (state != SupervisorState.RUNNING) {
            return;
        }
        try {
            startedServices().forEach(this::checkService);
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
     * <p>The services are snapshotted <i>before</i> the lock is taken, and that is not a detail:
     * every other lock in this class must be acquirable without {@code transitionLock}, or the
     * shutdown — which needs {@code transitionLock} first — can be blocked behind a service call
     * that has no deadline. See {@link #startedServices}.
     *
     * @throws DecommissionException if a decommission is already running, or the supervisor is in
     *                               no state to start one
     */
    private DecommissionCoordinator beginDecommission() throws DecommissionException {
        Map<String, EmbeddedService> services = startedServices();
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

    /**
     * The catch-up exclusion itself, on a thread of its own.
     *
     * <p>Two things have to be true before the call is made, and one of them is not a guard but a
     * registration. The guard is that nothing else has taken over the OpenSearch node meanwhile —
     * the decision to start this thread was taken on the reporting thread, and a supervisor-driven
     * decommission or a shutdown may have begun since. The registration is what makes the guard
     * worth anything: re-reading the state and then calling into OpenSearch is a check-then-act,
     * and the act a shutdown would interleave is {@code stop()} followed by {@code close()} on the
     * loader this thread is executing out of. So both happen under {@link #transitionLock}, which
     * the shutdown also takes before it moves to {@code STOPPING}: either this thread registers
     * first and the shutdown waits for it, or the shutdown wins and this thread sees {@code
     * STOPPING} and never makes the call.
     */
    private void excludeAfterExternalDecommission(String serviceName, RingStateEvent event) {
        // Read outside transitionLock: `started` is a synchronizedMap whose monitor the health
        // monitor competes for, and taking it under transitionLock is the lock ordering that
        // wedged the shutdown. See startedServices().
        EmbeddedService opensearch = startedServices().get("opensearch");
        Duration timeout = configuration.decommission().shardRelocationTimeout();
        ExternalCatchUp exclusion = new ExternalCatchUp(timeout);
        synchronized (transitionLock) {
            if (decommissionInFlight.get() != null || state != SupervisorState.RUNNING) {
                LOG.debug("Service '{}' reported {}, but the supervisor moved to {} before the"
                                + " catch-up exclusion could start; leaving it to whatever is"
                                + " driving this now", serviceName, event.state(), state);
                return;
            }
            if (opensearch == null) {
                LOG.warn("Service '{}' is {}: Cassandra is leaving the ring but was not asked"
                                + " through the supervisor. No OpenSearch service is running here,"
                                + " so there is nothing to exclude.", serviceName, event.state());
                return;
            }
            externalExclusion.set(exclusion);
        }
        try {
            LOG.warn("Service '{}' is {}: Cassandra is leaving the ring but was not asked through"
                            + " the supervisor; OpenSearch shard relocation may not complete before"
                            + " this process exits — use `bin/cassandra-opensearch decommission`,"
                            + " which relocates the shards before Cassandra gives its ranges back.",
                    serviceName, event.state());
            LOG.warn("Excluding this node from OpenSearch shard allocation as a best-effort"
                    + " catch-up. It is a catch-up and it may lose the race: watch `_cat/shards`"
                    + " and let this node empty before the process is stopped, or any shard with"
                    + " no other copy is lost with it.");
            // Bounded like every other call into a service. It was the one that was not, and an
            // exclusion that never returns is an exclusion nothing can wait for.
            TimeLimited.run("external-exclusion opensearch", timeout.plus(EXTERNAL_EXCLUSION_GRACE),
                    () -> opensearch.prepareDecommission(exclusion));
        } catch (TimeLimited.AbandonedException e) {
            abandon("opensearch", "a catch-up shard-allocation exclusion is still running inside"
                    + " it: " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Could not exclude this node from OpenSearch shard allocation after a"
                    + " decommission started outside the supervisor. Its shards will not relocate"
                    + " on their own; exclude the node by hand with a transient"
                    + " cluster.routing.allocation.exclude._name update.", e);
        } finally {
            exclusion.finish();
            externalExclusion.compareAndSet(exclusion, null);
        }
    }

    /**
     * The phase context for the catch-up exclusion, and the handle a shutdown waits on.
     *
     * <p>Never forced, because there is nothing here to force: this makes one call, {@code
     * prepareDecommission}, which does not decide whether data may be abandoned. The decision that
     * {@code force} would answer was taken by whoever ran {@code nodetool decommission}.
     *
     * <p>Cancellable, though, and it has to be: a shutdown that arrives while this is in flight
     * must be able to ask it to stop and then find out whether it did, which is what {@link
     * #finished} is for. It used to answer {@code false} to {@code isCancelled()} forever and be
     * registered nowhere, so a shutdown neither cancelled it nor waited for it.
     */
    private static final class ExternalCatchUp implements DecommissionContext {

        private final Duration timeout;
        private final CountDownLatch finished = new CountDownLatch(1);
        private volatile boolean cancelled;

        private ExternalCatchUp(Duration timeout) {
            this.timeout = timeout;
        }

        @Override
        public Duration timeout() {
            return timeout;
        }

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
            return cancelled;
        }

        void cancel() {
            cancelled = true;
        }

        void finish() {
            finished.countDown();
        }

        /** @return false if the call is still inside the service when the budget expires */
        boolean awaitCompletion(Duration budget) {
            try {
                return finished.await(budget.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return finished.getCount() == 0;
            }
        }
    }

    // --- observation -------------------------------------------------------------------------

    /** The started services, in startup order. */
    public Map<String, EmbeddedService> services() {
        return Collections.unmodifiableMap(startedServices());
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
