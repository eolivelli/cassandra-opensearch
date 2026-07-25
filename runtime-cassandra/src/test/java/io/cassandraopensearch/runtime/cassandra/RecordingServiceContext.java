/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import io.cassandraopensearch.spi.ServiceContext;

/** A {@link ServiceContext} that keeps everything the service reports, so tests can assert on it. */
final class RecordingServiceContext implements ServiceContext {

    record Event(String level, String type, String message) {
    }

    record FatalError(String message, String cause) {
    }

    private final Path home;
    private final Map<String, String> settings;
    private final List<Event> events = new CopyOnWriteArrayList<>();
    private final List<FatalError> fatalErrors = new CopyOnWriteArrayList<>();

    RecordingServiceContext(Path home, Map<String, String> settings) {
        this.home = home;
        this.settings = settings;
    }

    @Override
    public Path homeDirectory() {
        return home;
    }

    @Override
    public Path configFile() {
        return home.resolve("conf").resolve("cassandra.yaml");
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
        events.add(new Event(level, type, message));
    }

    @Override
    public void reportFatalError(String message, Throwable cause) {
        fatalErrors.add(new FatalError(message, String.valueOf(cause)));
    }

    List<Event> events() {
        return events;
    }

    List<FatalError> fatalErrors() {
        return fatalErrors;
    }
}
