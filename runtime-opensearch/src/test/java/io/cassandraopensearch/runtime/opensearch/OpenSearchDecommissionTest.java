/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The three decommission phases against a real single-node cluster.
 *
 * <p>A single node is the harshest case for the waiting phase: there is nowhere for a shard to
 * relocate to, so anything with data on it can only ever time out. That is exactly the
 * behaviour the supervisor depends on — {@code awaitDecommissionReady} must report failure and
 * let the operator decide, not throw and not block forever.
 */
class OpenSearchDecommissionTest {

    private static final int HTTP_PORT = 19220;
    private static final int TRANSPORT_PORT = 19320;
    private static final String NODE_NAME = "decommission-node";

    private final OpenSearchService service = new OpenSearchService();
    private final Rest rest = new Rest(HTTP_PORT);

    @BeforeEach
    void startNode(@TempDir Path home) throws Exception {
        service.start(new TestServiceContext(home, NODE_NAME, HTTP_PORT, TRANSPORT_PORT));
    }

    @AfterEach
    void stopNode() throws Exception {
        service.stop();
    }

    @Test
    void prepareAddsThisNodeToTheAllocationExclusionList() throws Exception {
        // Somebody else is already being retired; that exclusion must survive ours.
        assertThat(rest.put("/_cluster/settings", """
                {"transient":{"cluster.routing.allocation.exclude._name":"another-node"}}""").status())
                .isEqualTo(200);

        service.prepareDecommission(new TestDecommissionContext(Duration.ofSeconds(30), false));

        assertThat(service.status()).isEqualTo(ServiceStatus.DECOMMISSIONING);
        assertThat(rest.get("/_cluster/settings").body()).contains("another-node," + NODE_NAME);
    }

    @Test
    void awaitReturnsImmediatelyWhenTheNodeHoldsNoShards() throws Exception {
        TestDecommissionContext decommission = new TestDecommissionContext(Duration.ofSeconds(30), false);

        service.prepareDecommission(decommission);

        assertThat(service.awaitDecommissionReady(decommission)).isTrue();
        assertThat(decommission.progress())
                .extracting(TestDecommissionContext.Progress::message)
                .anyMatch(message -> message.contains("no shards on this node"));

        service.decommission(decommission);
        assertThat(service.status()).isEqualTo(ServiceStatus.DECOMMISSIONED);
    }

    @Test
    void awaitTimesOutWhenShardsHaveNowhereToGo() throws Exception {
        createIndexWithOneShard();
        TestDecommissionContext decommission = new TestDecommissionContext(Duration.ofSeconds(5), false);
        service.prepareDecommission(decommission);

        long startedAt = System.nanoTime();
        boolean ready = service.awaitDecommissionReady(decommission);

        assertThat(ready).isFalse();
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt))
                .isBetween(Duration.ofSeconds(4), Duration.ofSeconds(20));
        assertThat(decommission.progress())
                .extracting(TestDecommissionContext.Progress::message)
                .anyMatch(message -> message.equals("relocating 1 of 1 shards"));
    }

    @Test
    void awaitStopsEarlyWhenCancelled() throws Exception {
        createIndexWithOneShard();
        TestDecommissionContext decommission = new TestDecommissionContext(Duration.ofMinutes(10), false);
        service.prepareDecommission(decommission);
        decommission.cancel();

        assertThat(service.awaitDecommissionReady(decommission)).isFalse();
    }

    @Test
    void decommissionRefusesToDropShardsUnlessForced() throws Exception {
        createIndexWithOneShard();

        assertThatThrownBy(() -> service.decommission(new TestDecommissionContext(Duration.ofSeconds(30), false)))
                .isInstanceOf(ServiceException.class)
                .hasMessageContaining("1 shards are still allocated here");
        assertThat(service.status()).isNotEqualTo(ServiceStatus.DECOMMISSIONED);

        service.decommission(new TestDecommissionContext(Duration.ofSeconds(30), true));

        assertThat(service.status()).isEqualTo(ServiceStatus.DECOMMISSIONED);
    }

    /** DECOMMISSIONED is terminal and successful, so stopping afterwards must not erase it. */
    @Test
    void decommissionedSurvivesStop() throws Exception {
        TestDecommissionContext decommission = new TestDecommissionContext(Duration.ofSeconds(30), false);
        service.prepareDecommission(decommission);
        service.awaitDecommissionReady(decommission);
        service.decommission(decommission);

        service.stop();

        assertThat(service.status()).isEqualTo(ServiceStatus.DECOMMISSIONED);
    }

    private void createIndexWithOneShard() {
        assertThat(rest.put("/orders", """
                {"settings":{"number_of_shards":1,"number_of_replicas":0}}""").status()).isEqualTo(200);
        assertThat(rest.put("/orders/_doc/1?refresh=true", """
                {"item":"a shard that cannot move"}""").status()).isEqualTo(201);
    }
}
