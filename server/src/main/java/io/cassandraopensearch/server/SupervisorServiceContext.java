/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.server.config.ServiceConfiguration;
import io.cassandraopensearch.spi.ServiceContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * The {@link ServiceContext} handed to one isolated service.
 *
 * <p>Instances are created by the supervisor and therefore loaded by the application loader,
 * which is what lets an isolated service call back into the supervisor at all — the interface is
 * part of the shared SPI, so both sides see the same type.
 *
 * <p>Both callbacks are deliberately unable to throw. A service calling {@code reportEvent} from
 * inside its own startup path must not be handed an exception from the supervisor's logging: it
 * would surface as a startup failure whose stack trace points at the wrong subsystem entirely.
 */
final class SupervisorServiceContext implements ServiceContext {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorServiceContext.class);

    private final Path homeDirectory;
    private final ServiceConfiguration configuration;
    private final Map<String, String> settings;
    private final BiConsumer<String, Throwable> fatalErrorHandler;

    SupervisorServiceContext(
            Path homeDirectory,
            ServiceConfiguration configuration,
            BiConsumer<String, Throwable> fatalErrorHandler) {
        this(homeDirectory, configuration, Map.of(), fatalErrorHandler);
    }

    /**
     * @param derivedSettings settings the supervisor can only compute once the process is
     *                        running, layered over the configured ones. Today this is
     *                        OpenSearch's {@code node_name}, which is taken from the Cassandra
     *                        node's host id and therefore does not exist until Cassandra has
     *                        started.
     */
    SupervisorServiceContext(
            Path homeDirectory,
            ServiceConfiguration configuration,
            Map<String, String> derivedSettings,
            BiConsumer<String, Throwable> fatalErrorHandler) {
        this.homeDirectory = homeDirectory;
        this.configuration = configuration;
        Map<String, String> merged = new LinkedHashMap<>(configuration.settings());
        merged.putAll(derivedSettings);
        this.settings = Map.copyOf(merged);
        this.fatalErrorHandler = fatalErrorHandler;
        createDirectories();
    }

    /**
     * Creates the data and log directories up front so a service never has to, and so a
     * permissions problem is reported by the supervisor with a clear message rather than
     * surfacing as an obscure failure deep inside Cassandra's or OpenSearch's startup.
     */
    private void createDirectories() {
        try {
            Files.createDirectories(configuration.dataDirectory());
            Files.createDirectories(configuration.logsDirectory());
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Cannot create directories for service '" + configuration.name() + "': "
                            + e.getMessage(), e);
        }
    }

    @Override
    public Path homeDirectory() {
        return homeDirectory;
    }

    @Override
    public Path configFile() {
        return configuration.configFile();
    }

    @Override
    public Path configDirectory() {
        return homeDirectory.resolve("conf");
    }

    @Override
    public Path dataDirectory() {
        return configuration.dataDirectory();
    }

    @Override
    public Path logsDirectory() {
        return configuration.logsDirectory();
    }

    @Override
    public Map<String, String> settings() {
        return settings;
    }

    @Override
    public void reportEvent(String level, String type, String message) {
        String service = configuration.name();
        try {
            switch (level == null ? "INFO" : level.toUpperCase()) {
                case "ERROR" -> LOG.error("[{}] {}: {}", service, type, message);
                case "WARN" -> LOG.warn("[{}] {}: {}", service, type, message);
                default -> LOG.info("[{}] {}: {}", service, type, message);
            }
        } catch (RuntimeException e) {
            // Swallowed on purpose: see the class comment. A logging problem must not be able to
            // fail a service's startup.
        }
    }

    @Override
    public void reportFatalError(String message, Throwable cause) {
        LOG.error("[{}] fatal error, shutting down: {}", configuration.name(), message, cause);
        try {
            fatalErrorHandler.accept(configuration.name() + ": " + message, cause);
        } catch (RuntimeException e) {
            LOG.error("Fatal error handler itself failed", e);
        }
    }
}
