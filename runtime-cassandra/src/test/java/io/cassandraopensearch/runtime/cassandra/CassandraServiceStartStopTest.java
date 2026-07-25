/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.nio.file.Path;
import java.util.Map;

import io.cassandraopensearch.spi.ServiceStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The lifecycle transitions, each on its own freshly booted node. Kept apart from
 * {@link CassandraServiceLifecycleTest} because these tests destroy the node they use.
 */
class CassandraServiceStartStopTest {

    private TestNode node;

    @AfterEach
    void tearDown() throws Exception {
        if (node != null) {
            node.close();
            node = null;
        }
    }

    @Test
    void startsRunningAndStopsStopped(@TempDir Path home) throws Exception {
        node = TestNode.create(home);
        assertThat(node.service().status()).isEqualTo(ServiceStatus.NEW);

        node.service().start(node.context());
        assertThat(node.service().status()).isEqualTo(ServiceStatus.RUNNING);

        Map<String, String> details = node.details();
        assertThat(details).containsEntry("operationMode", "NORMAL")
                .containsEntry("nativeTransportActive", "true")
                .containsEntry("gossipActive", "true")
                .containsEntry("clusterName", "cassandra-opensearch-test");
        assertThat(details.get("hostId")).isNotBlank();

        node.service().stop();
        assertThat(node.service().status()).isEqualTo(ServiceStatus.STOPPED);
        assertThat(node.service().status().isTerminal()).isTrue();

        // details() stays cheap and answerable after a stop; it just has nothing left to report.
        assertThat(node.details()).containsEntry("status", "STOPPED");
    }

    @Test
    void stopIsIdempotent(@TempDir Path home) throws Exception {
        node = TestNode.started(home);

        node.service().stop();
        node.service().stop();
        node.service().stop();

        assertThat(node.service().status()).isEqualTo(ServiceStatus.STOPPED);
        // A second teardown must not re-run the steps, so nothing new can be reported as failing.
        assertThat(node.context().events())
                .filteredOn(event -> event.type().equals("cassandra.stopped"))
                .hasSize(1);
        // The teardown order in stop() is the one the spike measured as clean; if a step starts
        // failing, the order has drifted or Cassandra's API has.
        assertThat(node.context().events())
                .filteredOn(event -> event.type().equals("cassandra.shutdown.step.failed"))
                .isEmpty();
    }
}
