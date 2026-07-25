/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.server.config.NodeConfiguration;
import io.cassandraopensearch.spi.DecommissionContext;
import io.cassandraopensearch.spi.EmbeddedService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Retires this node from both clusters, in the one order that does not lose data.
 *
 * <p>The ordering is the entire point of this class, and it is not symmetric. An OpenSearch node
 * that still holds primary shards when the process exits loses them; so OpenSearch must finish
 * relocating <i>before</i> Cassandra leaves the ring, because once Cassandra has streamed its
 * ranges away the process is on its way out and nothing is going to wait for a shard. Reversing
 * these two steps produces a decommission that looks successful and silently drops indices.
 *
 * <pre>
 *   1. prepareDecommission(opensearch)  cluster.routing.allocation.exclude._name=&lt;node&gt;
 *      prepareDecommission(cassandra)   mark leaving, take no new ownership
 *   2. awaitDecommissionReady(opensearch)  poll until 0 shards remain here
 *   3. decommission(opensearch)         leave the cluster's allocation
 *      decommission(cassandra)          StorageService.decommission(), streams ranges
 *   4. stop(opensearch), stop(cassandra)
 * </pre>
 *
 * <p>Step 3 runs OpenSearch first for the same reason: it is the step that re-checks the shard
 * count and refuses (or, with {@code --force}, records) an unfinished hand-off, and that
 * decision has to be made while backing out is still free. Cassandra's is the irreversible one.
 *
 * <p>Every phase is bounded. {@code --timeout} from the CLI, when given, replaces the configured
 * per-phase limits rather than being spread across them: an operator who says "give up after ten
 * minutes" means each wait, not a tenth of one.
 */
public final class DecommissionCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(DecommissionCoordinator.class);

    /**
     * Added to the deadline the service was handed, when bounding the call from the outside.
     * The service enforces its own timeout and produces a far better message when it does — how
     * many shards are left, which operation mode the ring is in. This grace lets that message
     * win the race; the outer bound is only there for a call that has stopped honouring its
     * deadline altogether.
     */
    private static final Duration GRACE = Duration.ofSeconds(30);

    private final NodeConfiguration configuration;
    private final EmbeddedService cassandra;
    private final EmbeddedService opensearch;
    private final ProgressListener listener;

    private volatile boolean cancelled;

    /** Receives progress from whichever service is currently doing the waiting. */
    @FunctionalInterface
    public interface ProgressListener {
        void onProgress(String phase, int percentComplete, String message);
    }

    /**
     * @param services the services that are actually running, keyed by name. A disabled or
     *                 absent service is simply skipped — decommissioning a node that runs only
     *                 Cassandra is a legitimate configuration.
     */
    public DecommissionCoordinator(
            NodeConfiguration configuration,
            Map<String, EmbeddedService> services,
            ProgressListener listener) {
        this.configuration = configuration;
        this.cassandra = services.get("cassandra");
        this.opensearch = services.get("opensearch");
        this.listener = listener;
    }

    /** Asks the phase in flight to unwind at its next poll. */
    public void cancel() {
        cancelled = true;
    }

    /**
     * Runs the whole procedure and returns a one-line summary for the CLI.
     *
     * @param operatorTimeout {@code --timeout}, or null to use the configured per-phase limits
     * @param force           proceed even with shards outstanding, accepting the data loss
     * @throws DecommissionException if any phase failed or gave up. Everything before step 3 is
     *                               reversible, so the node is still up and serving when this is
     *                               thrown from phases 1 or 2.
     */
    public String run(Duration operatorTimeout, boolean force) throws DecommissionException {
        if (cassandra == null && opensearch == null) {
            throw new DecommissionException("Nothing to decommission: no service is running.");
        }
        Duration relocation = phaseTimeout(operatorTimeout, configuration.decommission().shardRelocationTimeout());
        Duration streaming = phaseTimeout(operatorTimeout, configuration.decommission().ringStreamingTimeout());
        List<String> summary = new ArrayList<>();

        // --- 1. prepare both, so the two hand-offs overlap instead of running back to back ---
        prepare("prepare-opensearch", opensearch, relocation, force);
        prepare("prepare-cassandra", cassandra, streaming, force);

        // --- 2. wait for OpenSearch to relocate its shards ---------------------------------
        if (opensearch != null) {
            if (awaitShardRelocation(relocation, force)) {
                summary.add("OpenSearch relocated its shards");
            } else if (force) {
                LOG.warn("Proceeding with --force: OpenSearch has shards outstanding and any copy"
                        + " that exists only on this node will be lost");
                summary.add("OpenSearch was forced out with shards outstanding");
            } else {
                throw new DecommissionException(
                        "OpenSearch did not finish relocating its shards within "
                                + TimeLimited.format(relocation)
                                + ". Cassandra has NOT left the ring and both services are still"
                                + " serving, but this node stays excluded from shard allocation."
                                + " Give the cluster more time and re-run, or re-run with --force"
                                + " to accept losing any shard with no other copy.");
            }
        }

        // --- 3. leave, OpenSearch first -----------------------------------------------------
        if (opensearch != null) {
            phase("decommission-opensearch", opensearch, relocation, force, EmbeddedService::decommission);
        }
        if (cassandra != null) {
            phase("decommission-cassandra", cassandra, streaming, force, EmbeddedService::decommission);
            summary.add("Cassandra streamed its ranges and left the ring");
        }

        // --- 4. stop, OpenSearch first ------------------------------------------------------
        stop("opensearch", opensearch);
        stop("cassandra", cassandra);

        String message = "decommission complete: " + String.join("; ", summary)
                + "; both services stopped";
        listener.onProgress("complete", 100, message);
        return message;
    }

    private void prepare(String phaseName, EmbeddedService service, Duration timeout, boolean force)
            throws DecommissionException {
        if (service != null) {
            phase(phaseName, service, timeout, force, EmbeddedService::prepareDecommission);
        }
    }

    /**
     * @return true when every shard has left this node, false when the service gave up with work
     *         outstanding — either its own deadline expired or the run was cancelled
     */
    private boolean awaitShardRelocation(Duration timeout, boolean force) throws DecommissionException {
        Phase phase = new Phase("await-shard-relocation", timeout, force);
        try {
            return TimeLimited.call(phase.name, timeout.plus(GRACE),
                    () -> opensearch.awaitDecommissionReady(phase));
        } catch (Exception e) {
            throw failure(phase.name, e);
        }
    }

    private void phase(String phaseName, EmbeddedService service, Duration timeout, boolean force, Step step)
            throws DecommissionException {
        Phase phase = new Phase(phaseName, timeout, force);
        LOG.info("Decommission phase '{}'", phaseName);
        try {
            TimeLimited.run(phaseName, timeout.plus(GRACE), () -> step.run(service, phase));
        } catch (Exception e) {
            throw failure(phaseName, e);
        }
    }

    /**
     * Stops a service that has already left its cluster. Failures are reported but do not abort
     * the shutdown: by this point the node is out of both clusters and the only remaining goal
     * is to release what the other service is still holding.
     */
    private void stop(String serviceName, EmbeddedService service) {
        if (service == null) {
            return;
        }
        listener.onProgress("stop-" + serviceName, -1, "stopping " + serviceName);
        try {
            TimeLimited.run("stop " + serviceName,
                    configuration.service(serviceName).shutdownTimeout(), service::stop);
        } catch (Exception e) {
            LOG.warn("Service '{}' did not stop cleanly after decommissioning; continuing", serviceName, e);
        }
    }

    private DecommissionException failure(String phaseName, Exception cause) {
        return new DecommissionException(
                "Decommission failed in phase '" + phaseName + "': " + cause.getMessage(), cause);
    }

    /**
     * {@code --timeout} replaces the configured limit for every waiting phase; without it, each
     * phase keeps the limit sized for what it actually waits on — shard relocation and ring
     * streaming are not the same order of magnitude.
     */
    private static Duration phaseTimeout(Duration operatorTimeout, Duration configured) {
        return operatorTimeout == null || operatorTimeout.isZero() || operatorTimeout.isNegative()
                ? configured
                : operatorTimeout;
    }

    @FunctionalInterface
    private interface Step {
        void run(EmbeddedService service, DecommissionContext context) throws Exception;
    }

    /** The {@link DecommissionContext} for one phase; the deadline and force flag are per phase. */
    private final class Phase implements DecommissionContext {

        private final String name;
        private final Duration timeout;
        private final boolean force;

        private Phase(String name, Duration timeout, boolean force) {
            this.name = name;
            this.timeout = timeout;
            this.force = force;
        }

        @Override
        public Duration timeout() {
            return timeout;
        }

        @Override
        public boolean force() {
            return force;
        }

        @Override
        public void reportProgress(int percentComplete, String message) {
            LOG.info("[{}] {}", name, message);
            listener.onProgress(name, percentComplete, message);
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }
    }
}
