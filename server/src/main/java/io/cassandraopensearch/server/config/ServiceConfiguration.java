/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Supervisor-level configuration for one embedded service.
 *
 * <p>The distinction that matters here is between the <b>keys an operator writes</b> in {@code
 * conf/cassandra-opensearch.yaml} and the <b>keys a runtime reads</b> from {@link #settings()}.
 * They are not the same vocabulary and were never meant to be: the YAML is written for a human
 * and groups everything about a service together, while {@code settings()} is a wire format
 * consumed by an implementation inside an isolated ClassLoader, which looks up exactly the
 * handful of names it needs and silently ignores anything else.
 *
 * <p>"Silently ignores" is why {@code ServiceSettingsTest} pins the emitted key set per service.
 * A renamed or misspelled key here does not fail anything — it just means the runtime falls back
 * to its own default, and the operator's setting quietly stops having an effect.
 */
public final class ServiceConfiguration {

    /**
     * The implementation class each service is booted through. These are the only two names that
     * cross from the supervisor into an isolated loader by string rather than by type, so they
     * are pinned here rather than being made configurable — a wrong value would only be
     * discovered at startup.
     */
    private static final Map<String, String> IMPLEMENTATIONS = Map.of(
            "cassandra", "io.cassandraopensearch.runtime.cassandra.CassandraService",
            "opensearch", "io.cassandraopensearch.runtime.opensearch.OpenSearchService");

    /**
     * The {@code shutdown_timeout} a service gets when the YAML does not name one — and the
     * fallback the supervisor uses if the configuration cannot be consulted at all, which is a
     * situation that only arises while the process is already going down.
     */
    public static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofMinutes(2);

    private static final Map<String, String> DEFAULT_CONFIG_FILES = Map.of(
            "cassandra", "cassandra.yaml",
            "opensearch", "opensearch.yml");

    private final String name;
    private final boolean enabled;
    private final Path configFile;
    private final Path libDirectory;
    private final Path dataDirectory;
    private final Path logsDirectory;
    private final Duration startupTimeout;
    private final Duration shutdownTimeout;
    private final Map<String, String> settings;

    private ServiceConfiguration(
            String name,
            boolean enabled,
            Path configFile,
            Path libDirectory,
            Path dataDirectory,
            Path logsDirectory,
            Duration startupTimeout,
            Duration shutdownTimeout,
            Map<String, String> settings) {
        this.name = name;
        this.enabled = enabled;
        this.configFile = configFile;
        this.libDirectory = libDirectory;
        this.dataDirectory = dataDirectory;
        this.logsDirectory = logsDirectory;
        this.startupTimeout = startupTimeout;
        this.shutdownTimeout = shutdownTimeout;
        this.settings = Map.copyOf(settings);
    }

    static ServiceConfiguration fromYaml(String name, YamlReader yaml, NodeConfiguration.Builder node) {
        boolean enabled = yaml.bool("enabled", true);
        String configFileName = yaml.string("config", DEFAULT_CONFIG_FILES.get(name));
        Duration startupTimeout = yaml.duration("startup_timeout", Duration.ofMinutes(5));
        Duration shutdownTimeout = yaml.duration("shutdown_timeout", DEFAULT_SHUTDOWN_TIMEOUT);

        Map<String, String> settings = new LinkedHashMap<>();
        List<String> known;

        if ("cassandra".equals(name)) {
            // Three keys, because CassandraService looks up exactly three. Its ports, listen
            // address and cluster name all come from conf/cassandra.yaml, which Cassandra parses
            // itself — the supervisor does not get to re-specify them, and emitting them here
            // would produce settings that look authoritative and change nothing.
            settings.put("jmx.address", yaml.string("jmx_address", "127.0.0.1"));
            settings.put("jmx.port", String.valueOf(yaml.port("jmx_port", 7199)));
            settings.put("startup.timeout.seconds", String.valueOf(startupTimeout.toSeconds()));
            known = List.of("enabled", "config", "jmx_address", "jmx_port", "native_transport_port",
                    "storage_port", "listen_address", "startup_timeout", "shutdown_timeout");
        } else {
            // OpenSearch, by contrast, is configured entirely from here: the supervisor owns port
            // allocation and node identity for the whole process, so these values are layered on
            // top of opensearch.yml rather than the other way round.
            settings.put("cluster_name", node.clusterName);
            settings.put("node_name", yaml.string("node_name", defaultNodeName()));
            settings.put("http_port", String.valueOf(yaml.port("http_port", 9200)));
            settings.put("transport_port", String.valueOf(yaml.port("transport_port", 9300)));
            settings.put("network_host", yaml.string("network_host", "127.0.0.1"));
            settings.put("start_timeout_seconds", String.valueOf(startupTimeout.toSeconds()));
            known = List.of("enabled", "config", "node_name", "http_port", "transport_port",
                    "network_host", "startup_timeout", "shutdown_timeout");
        }

        yaml.rejectUnknownKeys(known);

        return new ServiceConfiguration(
                name,
                enabled,
                node.configDirectory.resolve(configFileName),
                node.homeDirectory.resolve("lib").resolve(name),
                node.dataDirectory.resolve(name),
                node.logsDirectory,
                startupTimeout,
                shutdownTimeout,
                settings);
    }

    public String name() {
        return name;
    }

    /** When false the supervisor skips this service entirely — useful for isolating a problem. */
    public boolean enabled() {
        return enabled;
    }

    /** Fully-qualified {@code EmbeddedService} implementation loaded inside this service's loader. */
    public String implementationClass() {
        String implementation = IMPLEMENTATIONS.get(name);
        if (implementation == null) {
            throw new ConfigurationException("No implementation registered for service '" + name + "'.");
        }
        return implementation;
    }

    /** The service's native configuration file, e.g. {@code conf/cassandra.yaml}. */
    public Path configFile() {
        return configFile;
    }

    /** The directory whose jars form this service's isolated classpath. */
    public Path libDirectory() {
        return libDirectory;
    }

    public Path dataDirectory() {
        return dataDirectory;
    }

    public Path logsDirectory() {
        return logsDirectory;
    }

    public Duration startupTimeout() {
        return startupTimeout;
    }

    public Duration shutdownTimeout() {
        return shutdownTimeout;
    }

    /**
     * Supervisor-level settings handed to the service through {@code ServiceContext}, keyed
     * exactly as the corresponding runtime looks them up.
     *
     * <p>For {@code cassandra}: {@code jmx.address}, {@code jmx.port} and {@code
     * startup.timeout.seconds}. For {@code opensearch}: {@code cluster_name}, {@code node_name},
     * {@code http_port}, {@code transport_port}, {@code network_host} and {@code
     * start_timeout_seconds}.
     *
     * <p>{@code node_name} is a placeholder at this point. The supervisor replaces it with the
     * Cassandra node's host id once Cassandra is up — see {@code Supervisor} — because that is
     * the identity a decommission's shard exclusion keys on, and it must be stable across
     * restarts and unique among nodes that may share a hostname.
     */
    public Map<String, String> settings() {
        return settings;
    }

    /**
     * Node name to use when Cassandra has not (yet) supplied one — a disabled Cassandra service,
     * or a configuration read outside a running process. Matches the fallback OpenSearch would
     * pick for itself, so the value is never a surprise.
     */
    private static String defaultNodeName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "opensearch-node";
        }
    }
}
