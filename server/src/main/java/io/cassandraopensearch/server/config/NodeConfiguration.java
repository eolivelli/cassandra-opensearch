/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server.config;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The supervisor's own configuration, read from {@code conf/cassandra-opensearch.yaml}.
 *
 * <p>This file governs only what the supervisor decides: which services run, where their files
 * live, which ports they get, and how long each lifecycle phase may take. It deliberately does
 * <b>not</b> duplicate Cassandra's or OpenSearch's settings — those stay in their own native
 * {@code cassandra.yaml} and {@code opensearch.yml}, parsed by the servers themselves, so that
 * existing operational knowledge and tooling continue to apply unchanged.
 *
 * <p>Unknown keys are rejected rather than ignored. A silently-dropped setting in an operations
 * config file is a bad failure mode: the operator believes a limit is in force when it is not.
 */
public final class NodeConfiguration {

    private static final String DEFAULT_FILE_NAME = "cassandra-opensearch.yaml";

    private final Path homeDirectory;
    private final String clusterName;
    private final Path dataDirectory;
    private final Path logsDirectory;
    private final Path configDirectory;
    private final Map<String, ServiceConfiguration> services;
    private final DecommissionConfiguration decommission;
    private final Duration healthCheckInterval;

    private NodeConfiguration(Builder builder) {
        this.homeDirectory = builder.homeDirectory;
        this.clusterName = builder.clusterName;
        this.dataDirectory = builder.dataDirectory;
        this.logsDirectory = builder.logsDirectory;
        this.configDirectory = builder.configDirectory;
        this.services = Collections.unmodifiableMap(new LinkedHashMap<>(builder.services));
        this.decommission = builder.decommission;
        this.healthCheckInterval = builder.healthCheckInterval;
    }

    /**
     * Loads the configuration for an installation rooted at {@code homeDirectory}.
     *
     * @param homeDirectory the unpacked distribution root
     */
    public static NodeConfiguration load(Path homeDirectory) throws IOException {
        return load(homeDirectory, homeDirectory.resolve("conf").resolve(DEFAULT_FILE_NAME));
    }

    /** Loads from an explicit file, for tests and for {@code -Dcassandra-opensearch.config}. */
    public static NodeConfiguration load(Path homeDirectory, Path configFile) throws IOException {
        Objects.requireNonNull(homeDirectory, "homeDirectory");
        if (!Files.isRegularFile(configFile)) {
            throw new ConfigurationException(
                    "Configuration file not found: " + configFile.toAbsolutePath()
                            + ". An installation must provide conf/" + DEFAULT_FILE_NAME + ".");
        }
        Object parsed;
        try (InputStream in = Files.newInputStream(configFile)) {
            LoaderOptions options = new LoaderOptions();
            options.setAllowDuplicateKeys(false);
            parsed = new Yaml(new SafeConstructor(options)).load(in);
        } catch (YAMLException e) {
            // Malformed YAML and duplicate keys arrive here as snakeyaml's own exceptions. Left
            // unwrapped they escape past the caller's ConfigurationException handler, and the
            // operator gets a stack trace through the parser instead of a message naming the
            // file — for a class of mistake (a stray tab, a repeated key) where the position is
            // the whole diagnosis. So it is wrapped, and the position is kept.
            throw new ConfigurationException(describeYamlFailure(configFile, e), e);
        }
        if (parsed == null) {
            throw new ConfigurationException(configFile + " is empty.");
        }
        if (!(parsed instanceof Map)) {
            throw new ConfigurationException(
                    configFile + " must contain a YAML mapping at the top level, found "
                            + parsed.getClass().getSimpleName() + ".");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> root = (Map<String, Object>) parsed;
        return fromMap(homeDirectory, configFile, root);
    }

    /** Keeps snakeyaml's problem text and its line/column, which is the useful half of it. */
    private static String describeYamlFailure(Path configFile, YAMLException e) {
        if (!(e instanceof MarkedYAMLException marked)) {
            return configFile + " is not valid YAML: " + e.getMessage();
        }
        Mark mark = marked.getProblemMark();
        String where = mark == null
                ? ""
                : " at line " + (mark.getLine() + 1) + ", column " + (mark.getColumn() + 1);
        String context = marked.getContext() == null ? "" : marked.getContext() + ", ";
        return configFile + " is not valid YAML: " + context + marked.getProblem() + where + ".";
    }

    private static NodeConfiguration fromMap(Path home, Path source, Map<String, Object> root) {
        YamlReader reader = new YamlReader(source, root);
        Builder builder = new Builder();
        builder.homeDirectory = home;
        builder.clusterName = reader.string("cluster_name", "cassandra-opensearch");
        builder.configDirectory = home.resolve("conf");

        YamlReader directories = reader.section("directories", true);
        builder.dataDirectory = home.resolve(directories.string("data", "data"));
        builder.logsDirectory = home.resolve(directories.string("logs", "logs"));
        // Every other section rejects what it does not know; this one used not to, and a
        // mistyped 'data' quietly put the node's data under the installation directory.
        directories.rejectUnknownKeys(List.of("data", "logs"));

        YamlReader servicesSection = reader.section("services", false);
        for (String serviceName : List.of("cassandra", "opensearch")) {
            YamlReader service = servicesSection.section(serviceName, false);
            builder.services.put(serviceName, ServiceConfiguration.fromYaml(serviceName, service, builder));
        }
        servicesSection.rejectUnknownKeys(List.of("cassandra", "opensearch"));

        builder.decommission = DecommissionConfiguration.fromYaml(reader.section("decommission", true));

        YamlReader supervisor = reader.section("supervisor", true);
        builder.healthCheckInterval = supervisor.duration("health_check_interval", Duration.ofSeconds(10));
        supervisor.rejectUnknownKeys(List.of("health_check_interval"));

        reader.rejectUnknownKeys(
                List.of("cluster_name", "directories", "services", "decommission", "supervisor"));
        return new NodeConfiguration(builder);
    }

    public Path homeDirectory() {
        return homeDirectory;
    }

    public String clusterName() {
        return clusterName;
    }

    public Path dataDirectory() {
        return dataDirectory;
    }

    public Path logsDirectory() {
        return logsDirectory;
    }

    public Path configDirectory() {
        return configDirectory;
    }

    /** Service configurations in startup order: Cassandra first, then OpenSearch. */
    public Map<String, ServiceConfiguration> services() {
        return services;
    }

    public ServiceConfiguration service(String name) {
        ServiceConfiguration configuration = services.get(name);
        if (configuration == null) {
            throw new ConfigurationException("No configuration for service '" + name + "'.");
        }
        return configuration;
    }

    public DecommissionConfiguration decommission() {
        return decommission;
    }

    public Duration healthCheckInterval() {
        return healthCheckInterval;
    }

    static final class Builder {
        Path homeDirectory;
        String clusterName;
        Path dataDirectory;
        Path logsDirectory;
        Path configDirectory;
        final Map<String, ServiceConfiguration> services = new LinkedHashMap<>();
        DecommissionConfiguration decommission;
        Duration healthCheckInterval;
    }
}
