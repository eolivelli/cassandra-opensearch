/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import io.cassandraopensearch.spi.ServiceStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Start, observe, stop — against a real node on ports well clear of the OpenSearch defaults, so
 * a stray 9200 on the developer's machine cannot make these pass or fail by accident.
 */
class OpenSearchServiceLifecycleTest {

    private static final int HTTP_PORT = 19200;
    private static final int TRANSPORT_PORT = 19300;

    private final OpenSearchService service = new OpenSearchService();

    @AfterEach
    void stopService() throws Exception {
        service.stop();
    }

    @Test
    void reportsNewAndNothingElseBeforeStart() {
        assertThat(service.name()).isEqualTo("opensearch");
        assertThat(service.status()).isEqualTo(ServiceStatus.NEW);
        assertThat(service.details()).containsExactly(Map.entry("status", "NEW"));
    }

    @Test
    void startsRunsAndStops(@TempDir Path home) throws Exception {
        TestServiceContext context = new TestServiceContext(home, "lifecycle-node", HTTP_PORT, TRANSPORT_PORT);

        service.start(context);

        assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
        Map<String, String> details = service.details();
        assertThat(details)
                .containsEntry("status", "RUNNING")
                .containsEntry("node.name", "lifecycle-node")
                .containsEntry("cluster.name", "cassandra-opensearch-test")
                .containsEntry("shards.total", "0")
                .containsKeys("node.id", "http.address", "transport.address",
                        "cluster.health", "shards.primaries", "shards.relocating", "shards.initializing");
        assertThat(details.get("cluster.health")).isIn("GREEN", "YELLOW");
        assertThat(details.get("http.address")).contains(Integer.toString(HTTP_PORT));
        assertThat(details.get("transport.address")).contains(Integer.toString(TRANSPORT_PORT));
        assertThat(context.events()).anyMatch(event -> event.startsWith("INFO opensearch.started"));

        assertThat(new Rest(HTTP_PORT).get("/").status()).isEqualTo(200);

        service.stop();

        assertThat(service.status()).isEqualTo(ServiceStatus.STOPPED);
        assertThat(service.details()).containsEntry("status", "STOPPED");
    }

    /**
     * The restart path the spike recommends: a new {@code Node} inside the same ClassLoader.
     * If {@code stop()} left a socket, a thread pool or an mmap behind, binding the same ports
     * again is what would notice.
     */
    @Test
    void restartsInTheSameJvmOnTheSamePorts(@TempDir Path home) throws Exception {
        TestServiceContext context = new TestServiceContext(home, "restart-node", HTTP_PORT, TRANSPORT_PORT);

        service.start(context);
        String firstNodeId = service.details().get("node.id");
        service.stop();

        service.start(context);

        assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
        assertThat(new Rest(HTTP_PORT).get("/_cluster/health").status()).isEqualTo(200);
        // Same data directory, so the node keeps its identity across the restart.
        assertThat(service.details().get("node.id")).isEqualTo(firstNodeId);
    }

    /**
     * FAILED survives {@code stop()}, exactly as DECOMMISSIONED does — see {@code
     * OpenSearchDecommissionTest.decommissionedSurvivesStop}, and {@code CassandraService}, which
     * has treated the two alike from the start.
     *
     * <p>Two separate assignments could erase it, and only one of them was guarded: {@code
     * markStopped()} on the way out kept FAILED, while the STOPPING assignment on the way in
     * checked for DECOMMISSIONED alone and overwrote FAILED before {@code markStopped()} ever got
     * to see it. Nothing reaches that combination today — a node that is FAILED has no {@code Node}
     * left to close — but "unreachable" is a property of today's call sites, not of the class, and
     * the whole point of a status the supervisor reads after the final stop is that it says what
     * happened.
     *
     * <p>The status is set through the field because there is no honest way to make a running node
     * fail on demand; what is under test is the bookkeeping, not the failure.
     */
    @Test
    void failedSurvivesStop(@TempDir Path home) throws Exception {
        service.start(new TestServiceContext(home, "failed-node", HTTP_PORT, TRANSPORT_PORT));
        setStatus(ServiceStatus.FAILED);

        service.stop();

        assertThat(service.status()).isEqualTo(ServiceStatus.FAILED);
        assertThat(service.details()).containsEntry("status", "FAILED");
    }

    @SuppressWarnings("unchecked")
    private void setStatus(ServiceStatus status) throws Exception {
        Field field = OpenSearchService.class.getDeclaredField("status");
        field.setAccessible(true);
        ((AtomicReference<ServiceStatus>) field.get(service)).set(status);
    }

    @Test
    void stopIsIdempotent(@TempDir Path home) throws Exception {
        service.stop();
        assertThat(service.status()).isEqualTo(ServiceStatus.STOPPED);

        service.start(new TestServiceContext(home, "idempotent-node", HTTP_PORT, TRANSPORT_PORT));
        service.stop();
        service.stop();
        service.stop();

        assertThat(service.status()).isEqualTo(ServiceStatus.STOPPED);
    }
}
