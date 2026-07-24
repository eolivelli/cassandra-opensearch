/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.spi;

/**
 * Lifecycle state of one embedded service.
 *
 * <p>Instances of this enum cross the ClassLoader boundary, so this type is loaded by the
 * shared parent loader (see {@link EmbeddedService}).
 */
public enum ServiceStatus {
    /** Created but {@link EmbeddedService#start} has not been called. */
    NEW,
    /** {@code start()} is in progress. */
    STARTING,
    /** Started and serving traffic. */
    RUNNING,
    /** Refusing new work and flushing in-flight state, but still a cluster member. */
    DRAINING,
    /** Actively handing off its data to the rest of the cluster. */
    DECOMMISSIONING,
    /** No longer a cluster member; its data has been handed off. Terminal, successful. */
    DECOMMISSIONED,
    /** {@code stop()} is in progress. */
    STOPPING,
    /** Fully stopped. Terminal. */
    STOPPED,
    /** Startup or a lifecycle transition failed. Terminal. */
    FAILED;

    /** @return true if no further transition is expected. */
    public boolean isTerminal() {
        return this == DECOMMISSIONED || this == STOPPED || this == FAILED;
    }

    /** @return true if the service is able to serve client requests. */
    public boolean isServing() {
        return this == RUNNING;
    }
}
