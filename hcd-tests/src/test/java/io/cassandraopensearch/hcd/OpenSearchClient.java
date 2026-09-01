/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.hcd;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Thin OpenSearch REST client shared by all HCD integration tests.
 *
 * <p>Connection coordinates are read from system properties injected by the Maven
 * Surefire configuration in {@code hcd-tests/pom.xml}:
 * <ul>
 *   <li>{@code opensearch.host} — default {@code localhost}</li>
 *   <li>{@code opensearch.port} — default {@code 9200}</li>
 * </ul>
 */
final class OpenSearchClient {

    static final String HOST = System.getProperty("opensearch.host", "localhost");
    static final int PORT = Integer.parseInt(System.getProperty("opensearch.port", "9200"));

    private final String base;
    private final HttpClient http;

    OpenSearchClient() {
        this.base = "http://" + HOST + ":" + PORT;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    record Response(int status, String body) {
        boolean isSuccess() {
            return status >= 200 && status < 300;
        }
    }

    Response get(String path) {
        return send(HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .GET());
    }

    Response put(String path, String body) {
        return send(HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body)));
    }

    Response post(String path, String body) {
        return send(HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .POST(body == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body)));
    }

    Response delete(String path) {
        return send(HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(30))
                .DELETE());
    }

    private Response send(HttpRequest.Builder builder) {
        try {
            HttpResponse<String> response = http.send(
                    builder.build(), HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for OpenSearch", e);
        }
    }
}
