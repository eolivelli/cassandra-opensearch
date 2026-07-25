/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the node over HTTP. One node for the whole class: these tests only read and write
 * documents, and paying three seconds of cold start per assertion buys nothing.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpenSearchRestApiTest {

    private static final int HTTP_PORT = 19210;
    private static final int TRANSPORT_PORT = 19310;

    private final OpenSearchService service = new OpenSearchService();
    private final Rest rest = new Rest(HTTP_PORT);

    @BeforeAll
    void startNode(@TempDir Path home) throws Exception {
        service.start(new TestServiceContext(home, "rest-node", HTTP_PORT, TRANSPORT_PORT));
    }

    @AfterAll
    void stopNode() throws Exception {
        service.stop();
    }

    @Test
    void indexesAndSearchesADocument() {
        assertThat(rest.put("/books", """
                {"settings":{"number_of_shards":1,"number_of_replicas":0}}""").status())
                .isEqualTo(200);
        assertThat(rest.put("/books/_doc/1", """
                {"title":"the embedded node","pages":42}""").status())
                .isEqualTo(201);
        assertThat(rest.post("/books/_refresh", null).status()).isEqualTo(200);

        Rest.Response search = rest.post("/books/_search", """
                {"query":{"match":{"title":"embedded"}}}""");

        assertThat(search.status()).isEqualTo(200);
        assertThat(search.body())
                .contains("\"_id\":\"1\"")
                .contains("the embedded node")
                .doesNotContain("\"hits\":{\"total\":{\"value\":0");

        // The shard the search just answered from has to show up in details(), which derives it
        // from the applied cluster state rather than asking the cluster.
        assertThat(service.details())
                .containsEntry("cluster.health", "GREEN")
                .containsEntry("shards.total", "1")
                .containsEntry("shards.primaries", "1")
                .containsEntry("shards.relocating", "0");
    }

    /**
     * The regression test for the spike's most expensive finding.
     *
     * <p>These three endpoints, and only these, break when OpenSearch runs in a child
     * ClassLoader whose thread context ClassLoader still points at the application loader:
     * {@code XContentBuilder} resolves its {@code XContentBuilderExtension} providers through
     * the one-argument {@code ServiceLoader.load}, finds none, and caches the gap in a
     * {@code static final} map for the life of the loader. The node then reports healthy,
     * indexes and searches perfectly, and answers these calls with
     * {@code HTTP 400 "no raw transformer found for class ...TimeValue"}.
     *
     * <p>{@link IsolatedClassLoaderRestApiTest} runs the same assertions with the isolation
     * actually in place; this one guards the flat-classpath case.
     */
    @Test
    void serialisesTheEndpointsThatDependOnXContentExtensions() {
        for (String path : new String[] {
                "/_cluster/health?timeout=30s",
                "/_nodes/stats",
                "/_cluster/stats" }) {
            Rest.Response response = rest.get(path);
            assertThat(response.status()).as("GET %s -> %s", path, response.body()).isEqualTo(200);
            assertThat(response.body()).doesNotContain("no raw transformer found");
        }
    }
}
