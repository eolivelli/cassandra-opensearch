/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.util.concurrent.atomic.AtomicReference;

import io.cassandraopensearch.spi.ServiceContext;

import org.apache.cassandra.utils.JVMKiller;

/**
 * Turns Cassandra's {@code System.exit(100)} into a report to the supervisor.
 *
 * <p>{@code JVMStabilityInspector.Killer} kills the JVM on ordinary operational faults —
 * {@code OutOfMemoryError}, {@code FSError}, a corrupt sstable, or a {@code disk_failure_policy} /
 * {@code commit_failure_policy} of {@code die}. This path is <b>not</b> covered by
 * {@code CassandraDaemon}'s {@code runManaged} flag, which only guards {@code exitOrFail}; missing
 * it leaves a latent whole-process kill that would take OpenSearch down with Cassandra.
 *
 * <p>The event is not swallowed. Cassandra has concluded the node is unusable, so the correct
 * response is still to shut the process down — but by the supervisor, in order, giving OpenSearch a
 * chance to relocate its shards. That is exactly what
 * {@link ServiceContext#reportFatalError(String, Throwable)} is for.
 */
final class InterceptingKiller implements JVMKiller {

    private final ServiceContext context;
    private final AtomicReference<Throwable> lastAttempt = new AtomicReference<>();

    InterceptingKiller(ServiceContext context) {
        this.context = context;
    }

    @Override
    public void killJVM(Throwable cause, boolean quiet) {
        lastAttempt.set(cause == null ? new IllegalStateException("Cassandra requested JVM exit") : cause);
        context.reportFatalError(
                "Cassandra asked to terminate the JVM (System.exit(100)); intercepted because the"
                        + " process is shared with OpenSearch. The supervisor must shut down in order.",
                cause);
    }

    /** The throwable behind the most recent intercepted kill request, or null if there was none. */
    Throwable lastKillAttempt() {
        return lastAttempt.get();
    }
}
