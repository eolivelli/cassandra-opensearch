/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * OpenSearch's REST API over real HTTP.
 *
 * <p>Real HTTP rather than the transport client, because the REST layer is where this product's
 * most dangerous failure mode lives: under a child ClassLoader with an unpinned thread context
 * ClassLoader, {@code XContentBuilder}'s writer table comes up empty and a set of endpoints
 * answers 400 while indexing and search stay perfectly healthy. Nothing below HTTP sees that.
 */
final class Rest {

    private final String base;
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    Rest(String host, int port) {
        this.base = "http://" + host + ':' + port;
    }

    record Response(int status, String body) {
    }

    Response get(String path) {
        return send(request(path).GET());
    }

    Response put(String path, String body) {
        return send(request(path).PUT(HttpRequest.BodyPublishers.ofString(body)));
    }

    Response post(String path, String body) {
        return send(request(path).POST(HttpRequest.BodyPublishers.ofString(body)));
    }

    Response delete(String path) {
        return send(request(path).DELETE());
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json");
    }

    private Response send(HttpRequest.Builder builder) {
        try {
            HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", e);
        }
    }

    /**
     * The plain-text {@code _cat} APIs, which need no JSON parser to assert against.
     *
     * <p>A non-200 raises an exception rather than an {@link AssertionError} so that
     * {@link Await} treats it as "not yet" and keeps polling; a cluster that never answers still
     * fails, at the deadline, with the last status it gave.
     */
    String cat(String path) {
        Response response = get(path.contains("?") ? path : path + "?v=true");
        if (response.status() != 200) {
            throw new IllegalStateException(
                    "GET " + path + " returned " + response.status() + ": " + response.body());
        }
        return response.body();
    }
}
