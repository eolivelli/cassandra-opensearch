/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.hcd;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the HCD + OpenSearch stack defined in {@code docker-compose-full.yaml} is
 * reachable before any heavier tests run.
 *
 * <p>This is deliberately the thinnest possible test: one CQL ping and one HTTP ping. If either
 * fails the failure message names the host and port so the operator knows exactly what is not up,
 * rather than seeing a confusing driver timeout or connection-refused buried in a longer test.
 *
 * <p>Connection coordinates come from system properties set by the Maven Surefire configuration
 * in {@code hcd-tests/pom.xml}. Defaults match the ports published by docker-compose-full.yaml:
 * CQL on {@code localhost:9042}, OpenSearch on {@code localhost:9200}.
 *
 * <p>Run with the stack already up:
 * <pre>{@code
 *   mvn test -pl hcd-tests -Phcd-tests
 * }</pre>
 */
class StackConnectivityTest {

    private static final String CQL_HOST     = System.getProperty("hcd.host",       "localhost");
    private static final int    CQL_PORT     = Integer.parseInt(System.getProperty("hcd.cql.port",  "9042"));
    private static final String DATACENTER   = System.getProperty("hcd.datacenter", "datacenter1");

    /**
     * Opens a CQL session, executes {@code SELECT release_version FROM system.local},
     * and asserts the result is non-null.
     *
     * <p>{@code system.local} is always present and always returns exactly one row; it is the
     * canonical liveness check for a Cassandra-compatible node and requires no keyspace setup.
     */
    @Test
    void cassandraIsReachableOverCql() {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(CQL_HOST, CQL_PORT))
                .withLocalDatacenter(DATACENTER)
                .build()) {

            Row row = session.execute("SELECT release_version FROM system.local").one();

            assertThat(row)
                    .as("system.local must return exactly one row on %s:%d", CQL_HOST, CQL_PORT)
                    .isNotNull();
            assertThat(row.getString("release_version"))
                    .as("release_version must be a non-blank string")
                    .isNotBlank();

            System.out.println("Connected to Cassandra at " + CQL_HOST + ":" + CQL_PORT
                    + "  release_version=" + row.getString("release_version"));
        }
    }

    /**
     * Issues {@code GET /_cluster/health} and asserts the cluster status is GREEN or YELLOW.
     *
     * <p>RED means at least one primary shard is unassigned. For a freshly started single-node
     * stack with no data that should not happen, but it would indicate the node is not ready to
     * accept index operations, which would make every subsequent test misleading.
     */
    @Test
    void openSearchIsReachableOverHttp() {
        OpenSearchClient client = new OpenSearchClient();

        OpenSearchClient.Response response = client.get("/_cluster/health");

        assertThat(response.status())
                .as("GET /_cluster/health on %s:%d returned HTTP %d: %s",
                        OpenSearchClient.HOST, OpenSearchClient.PORT,
                        response.status(), response.body())
                .isEqualTo(200);
        assertThat(response.body())
                .as("cluster health must not be RED: %s", response.body())
                .contains("\"status\":")
                .doesNotContain("\"status\":\"red\"");

        System.out.println("Connected to OpenSearch at "
                + OpenSearchClient.HOST + ":" + OpenSearchClient.PORT
                + "  health=" + response.body());
    }
}
