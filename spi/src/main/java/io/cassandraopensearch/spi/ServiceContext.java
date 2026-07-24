/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.spi;

import java.nio.file.Path;
import java.util.Map;

/**
 * Everything an {@link EmbeddedService} is given at startup: where its files live, its
 * configuration, and the callbacks it uses to talk back to the supervisor.
 *
 * <p>Implemented by the supervisor and loaded by the shared parent loader, so an isolated
 * service may call it freely.
 */
public interface ServiceContext {

    /** Installation root of the whole product (the unpacked tarball). Read-only. */
    Path homeDirectory();

    /**
     * The service's own configuration file: {@code conf/cassandra.yaml} or {@code
     * conf/opensearch.yml}. Guaranteed to exist and be readable.
     */
    Path configFile();

    /** Directory the service owns for its configuration files. */
    Path configDirectory();

    /** Directory the service owns for persistent data. Created if absent. */
    Path dataDirectory();

    /** Directory the service owns for its logs. Created if absent. */
    Path logsDirectory();

    /**
     * Supervisor-level settings for this service from {@code conf/cassandra-opensearch.yaml},
     * flattened to dotted keys. These are the settings the supervisor itself understands
     * (ports, timeouts, coupling policy) — not the service's native configuration file, which
     * the service parses itself from {@link #configFile()}.
     */
    Map<String, String> settings();

    /**
     * Reports a lifecycle or health event to the supervisor. Safe to call from any thread, at
     * any time, including from inside {@link EmbeddedService#start}. Never throws and never
     * blocks: the supervisor queues the event.
     *
     * @param level one of {@code "INFO"}, {@code "WARN"}, {@code "ERROR"}
     * @param type  a short machine-readable event key, e.g. {@code "ring.state.changed"}
     * @param message human-readable detail
     */
    void reportEvent(String level, String type, String message);

    /**
     * Signals that this service has failed unrecoverably and the supervisor should shut the
     * whole process down. Used for asynchronous failures discovered after {@code start()} has
     * already returned — an uncaught error on a background thread, for instance.
     */
    void reportFatalError(String message, Throwable cause);
}
