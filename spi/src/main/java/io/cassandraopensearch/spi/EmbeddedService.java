/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.spi;

import java.util.Map;

/**
 * One server (Cassandra or OpenSearch) embedded in this JVM.
 *
 * <p><b>ClassLoader contract.</b> Implementations are loaded <i>inside</i> an isolated child
 * ClassLoader that cannot see the supervisor's classes; the supervisor holds a reference to
 * this interface, which is loaded by the shared parent loader. Consequently <b>every type
 * appearing in a signature on this interface must be either a JDK type or a type from the
 * {@code io.cassandraopensearch.spi} package</b>. Returning, say, a Cassandra {@code
 * TokenMetadata} would throw {@link NoClassDefFoundError} in the supervisor.
 *
 * <p><b>Threading.</b> The supervisor calls these methods from a single lifecycle thread and
 * never concurrently, with the exception of {@link #status()} and {@link #details()}, which may
 * be called at any time from any thread and must therefore be cheap and non-blocking.
 *
 * <p>Implementations are discovered by fully-qualified class name (see {@code
 * ServiceDescriptor#implementationClass}) and instantiated via their public no-arg constructor
 * inside the isolated loader. The constructor must do nothing but allocate; all real work
 * belongs in {@link #start}.
 */
public interface EmbeddedService {

    /** Stable short name used in logs, JMX and CLI output, e.g. {@code "cassandra"}. */
    String name();

    /**
     * Boots the server and blocks until it is ready to serve traffic, or throws.
     *
     * <p>The calling thread's context ClassLoader is already set to the isolated loader.
     *
     * @param context configuration and callbacks into the supervisor
     * @throws Exception if startup fails; the supervisor moves the service to
     *                   {@link ServiceStatus#FAILED} and aborts the whole process
     */
    void start(ServiceContext context) throws Exception;

    /** Current state. Must not block. */
    ServiceStatus status();

    /**
     * Human-readable diagnostics for {@code status} CLI output and health endpoints, e.g.
     * cluster name, node id, listen addresses, ring/shard state. Must not block.
     *
     * @return a stable-ordered map; never null, may be empty
     */
    Map<String, String> details();

    /**
     * Phase 1 of decommissioning: stop accepting new ownership and prepare to hand data off.
     *
     * <p>For OpenSearch this excludes the node from shard allocation so the cluster starts
     * relocating its shards elsewhere. For Cassandra this is a no-op preparation step.
     *
     * <p>This method must return promptly; the long wait belongs in {@link
     * #awaitDecommissionReady}. Splitting the two is what lets the supervisor overlap
     * OpenSearch shard relocation with Cassandra's own streaming.
     */
    void prepareDecommission(DecommissionContext context) throws Exception;

    /**
     * Blocks until this service has finished handing off the data it owns, or the deadline in
     * {@code context} expires.
     *
     * @return true if hand-off completed, false if the deadline expired with work outstanding
     */
    boolean awaitDecommissionReady(DecommissionContext context) throws Exception;

    /**
     * Undoes {@link #prepareDecommission}: the compensating action for a decommission that
     * failed, was refused or was cancelled before the point of no return.
     *
     * <p>Without it a refusal is not free. OpenSearch's preparation excludes this node from
     * shard allocation cluster-wide, and a preparation that is never undone leaves a node that
     * still serves what it holds but will never be given a shard again — on a single-node
     * cluster, every index created afterwards stays UNASSIGNED forever. The supervisor refuses a
     * single-node decommission in {@link #prepareDecommission} on Cassandra, which runs
     * <i>after</i> OpenSearch's, so this is the only thing standing between one mistyped command
     * and a cluster that looks unrecoverable.
     *
     * <p>Must be safe to call when {@link #prepareDecommission} never ran, ran and failed
     * halfway, or has already been undone: the supervisor calls it for every service it touched,
     * and it cannot know how far each one got. Restore the status the service reported before
     * the preparation, and change nothing that {@link #decommission} owns — this runs only while
     * leaving is still free.
     */
    void abortDecommission(DecommissionContext context) throws Exception;

    /**
     * Phase 2: leave the cluster for good. After this returns normally the service must report
     * {@link ServiceStatus#DECOMMISSIONED} and own no data.
     */
    void decommission(DecommissionContext context) throws Exception;

    /**
     * Stops the server and releases every resource it holds: threads, sockets, file handles,
     * memory-mapped files, JMX registrations and any timer it started.
     *
     * <p>Must be idempotent, and must not call {@link System#exit} — the JVM is shared. After
     * this returns, the supervisor discards the isolated ClassLoader, so any thread still
     * running implementation code would pin the loader and leak it.
     */
    void stop() throws Exception;
}
