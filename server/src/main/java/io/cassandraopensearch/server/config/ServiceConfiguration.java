/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Supervisor-level configuration for one embedded service. */
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
        Map<String, String> settings = new LinkedHashMap<>();
        List<String> known;

        if ("cassandra".equals(name)) {
            settings.put("jmx_port", String.valueOf(yaml.port("jmx_port", 7199)));
            settings.put("native_transport_port", String.valueOf(yaml.port("native_transport_port", 9042)));
            settings.put("storage_port", String.valueOf(yaml.port("storage_port", 7000)));
            settings.put("listen_address", yaml.string("listen_address", "127.0.0.1"));
            known = List.of("enabled", "config", "jmx_port", "native_transport_port",
                    "storage_port", "listen_address", "startup_timeout", "shutdown_timeout");
        } else {
            settings.put("http_port", String.valueOf(yaml.port("http_port", 9200)));
            settings.put("transport_port", String.valueOf(yaml.port("transport_port", 9300)));
            settings.put("network_host", yaml.string("network_host", "127.0.0.1"));
            known = List.of("enabled", "config", "http_port", "transport_port",
                    "network_host", "startup_timeout", "shutdown_timeout");
        }

        boolean enabled = yaml.bool("enabled", true);
        String configFileName = yaml.string("config", DEFAULT_CONFIG_FILES.get(name));
        Duration startupTimeout = yaml.duration("startup_timeout", Duration.ofMinutes(5));
        Duration shutdownTimeout = yaml.duration("shutdown_timeout", Duration.ofMinutes(2));
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

    /** Supervisor-level settings handed to the service through {@code ServiceContext}. */
    public Map<String, String> settings() {
        return settings;
    }
}
