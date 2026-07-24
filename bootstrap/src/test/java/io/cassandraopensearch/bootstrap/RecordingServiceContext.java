/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.bootstrap;

import io.cassandraopensearch.spi.ServiceContext;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A {@link ServiceContext} that records what the service reported, for assertions. */
final class RecordingServiceContext implements ServiceContext {

    private final Path home = Paths.get(System.getProperty("java.io.tmpdir"), "probe-home");
    private final Map<String, String> settings = new LinkedHashMap<>();
    private final List<String> events = new ArrayList<>();
    private volatile String fatalError;

    List<String> events() {
        return events;
    }

    String fatalError() {
        return fatalError;
    }

    @Override
    public Path homeDirectory() {
        return home;
    }

    @Override
    public Path configFile() {
        return home.resolve("conf/probe.yaml");
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
        fatalError = message;
    }
}
