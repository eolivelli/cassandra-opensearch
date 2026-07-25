/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.spi.EmbeddedService;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link SupervisorControlMBean} over a live {@link Supervisor}.
 *
 * <p>A thin adapter on purpose. Everything it exposes is already a supervisor method; its only
 * real work is flattening the SPI's types into the JDK types a remote client can deserialize,
 * and turning {@code 0} — which is what a CLI sends when the operator gave no {@code --timeout} —
 * into "use the configured limits".
 */
public final class SupervisorControl implements SupervisorControlMBean {

    private final Supervisor supervisor;

    SupervisorControl(Supervisor supervisor) {
        this.supervisor = supervisor;
    }

    @Override
    public String getState() {
        return supervisor.state().name();
    }

    @Override
    public String getFailureMessage() {
        String message = supervisor.failureMessage();
        return message == null ? "" : message;
    }

    @Override
    public String getClusterName() {
        return supervisor.configuration().clusterName();
    }

    @Override
    public String[] getServiceNames() {
        return supervisor.services().keySet().toArray(new String[0]);
    }

    @Override
    public Map<String, String> getServiceStatuses() {
        Map<String, String> statuses = new LinkedHashMap<>();
        supervisor.services().forEach((name, service) -> statuses.put(name, service.status().name()));
        return statuses;
    }

    @Override
    public Map<String, String> serviceDetails(String service) {
        EmbeddedService instance = supervisor.services().get(service);
        if (instance == null) {
            return Map.of("error", "no service named '" + service + "' is running");
        }
        // Copied into a plain LinkedHashMap: details() may return a Map implementation that only
        // exists inside the isolated loader, which no remote client could deserialize.
        return new LinkedHashMap<>(instance.details());
    }

    @Override
    public String getDecommissionProgress() {
        return supervisor.decommissionProgress().toString();
    }

    @Override
    public String decommission(long timeoutSeconds, boolean force) throws DecommissionException {
        Duration timeout = timeoutSeconds > 0 ? Duration.ofSeconds(timeoutSeconds) : null;
        return supervisor.decommission(timeout, force);
    }
}
