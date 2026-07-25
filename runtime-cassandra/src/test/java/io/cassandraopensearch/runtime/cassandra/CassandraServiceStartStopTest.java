/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.nio.file.Files;
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

    /**
     * The {@code GCInspector} comes off the platform garbage-collector MXBeans before {@code
     * drain()} runs, and not merely at some point during the teardown.
     *
     * <p>Order is the whole content of the claim. The inspector is a notification listener on
     * JVM-global beans, and on an old-generation collection it calls
     * {@code LifecycleTransaction.rescheduleFailedDeletions()}, which submits to executors {@code
     * drain()} has already stopped. The platform beans dispatch to their listeners in a bare loop
     * with no per-listener {@code try}, so the {@code RejectedExecutionException} that follows also
     * stops every listener registered behind it — in a process running a second node, that is the
     * live node's inspector going quiet. Detaching from inside {@code jmx.stop()}, the third
     * teardown step, left that window open for the whole of {@code drain()}.
     *
     * <p>Asserted through the node's own log because that is where the ordering is visible after
     * the fact, and because both markers are written by the code whose order is in question.
     */
    @Test
    void theGcInspectorIsDetachedBeforeDrainShutsTheExecutorsDown(@TempDir Path home) throws Exception {
        node = TestNode.started(home);

        node.service().stop();

        String log = Files.readString(node.logFile());
        int detached = log.indexOf("detached from the platform garbage-collector MXBeans");
        int draining = log.indexOf("DRAINING: starting drain process");
        assertThat(detached).as("the detach is not logged at all; see NodeMBeanWrapper").isNotNegative();
        assertThat(draining).as("drain() did not run; the teardown changed shape").isNotNegative();
        assertThat(detached)
                .as("the GCInspector must be off the platform beans before drain() stops the"
                        + " executor its notification handler submits to")
                .isLessThan(draining);
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
