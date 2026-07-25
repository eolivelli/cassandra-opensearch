/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

/**
 * State of the process as a whole, as opposed to {@link io.cassandraopensearch.spi.ServiceStatus},
 * which describes one embedded service.
 *
 * <p>The two are deliberately separate. A supervisor in {@code RUNNING} says the coupled
 * lifecycle completed and the MBean is answering; it says nothing about whether OpenSearch has
 * gone yellow since. The CLI shows both.
 */
public enum SupervisorState {
    /** Constructed; {@link Supervisor#start()} has not been called. */
    NEW,
    /** Bringing services up, in order. */
    STARTING,
    /** Every enabled service reached {@code RUNNING} and the supervisor MBean is registered. */
    RUNNING,
    /** A coupled decommission is in flight. */
    DECOMMISSIONING,
    /** Taking services down, in reverse order. */
    STOPPING,
    /** Everything stopped cleanly; the process is on its way out with exit code 0. */
    STOPPED,
    /** Startup failed, or a service asked for the process to die. Exit code is non-zero. */
    FAILED
}
