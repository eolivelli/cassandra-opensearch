/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import io.cassandraopensearch.spi.ServiceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The supervisor's half of the contract, for tests: a real directory layout under a
 * {@code @TempDir} and an {@code opensearch.yml} the service has to parse.
 *
 * <p>{@code discovery.type: single-node} is written into that file rather than passed as a
 * supervisor setting on purpose — it exercises the layering, and it is where a real deployment
 * would put it.
 */
final class TestServiceContext implements ServiceContext {

    private final Path home;
    private final Map<String, String> settings;
    private final List<String> events = new CopyOnWriteArrayList<>();

    TestServiceContext(Path home, String nodeName, int httpPort, int transportPort) {
        this.home = home;
        this.settings = new LinkedHashMap<>(Map.of(
                OpenSearchService.SETTING_NODE_NAME, nodeName,
                OpenSearchService.SETTING_HTTP_PORT, Integer.toString(httpPort),
                OpenSearchService.SETTING_TRANSPORT_PORT, Integer.toString(transportPort),
                OpenSearchService.SETTING_NETWORK_HOST, "127.0.0.1",
                OpenSearchService.SETTING_CLUSTER_NAME, "cassandra-opensearch-test",
                OpenSearchService.SETTING_START_TIMEOUT, "120"));
        try {
            Files.createDirectories(configDirectory());
            Files.createDirectories(dataDirectory());
            Files.createDirectories(logsDirectory());
            Files.writeString(configFile(), """
                    discovery.type: single-node
                    node.attr.embedded: true
                    """);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Map<String, String> mutableSettings() {
        return settings;
    }

    List<String> events() {
        return events;
    }

    @Override
    public Path homeDirectory() {
        return home;
    }

    @Override
    public Path configFile() {
        return configDirectory().resolve("opensearch.yml");
    }

    @Override
    public Path configDirectory() {
        return home.resolve("conf");
    }

    @Override
    public Path dataDirectory() {
        return home.resolve("data");
    }

    @Override
    public Path logsDirectory() {
        return home.resolve("logs");
    }

    @Override
    public Map<String, String> settings() {
        return settings;
    }

    @Override
    public void reportEvent(String level, String type, String message) {
        events.add(level + " " + type + " " + message);
    }

    @Override
    public void reportFatalError(String message, Throwable cause) {
        events.add("FATAL " + message + " " + cause);
    }
}
