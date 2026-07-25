/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * A minimal REST client, so the tests talk to the node the way an operator does — over HTTP,
 * through the netty4 module — rather than through the in-process {@code Client}, which would
 * bypass the whole REST and XContent layer these tests exist to check.
 */
record Rest(int port) {

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    record Response(int status, String body) {
    }

    Response get(String path) {
        return send("GET", path, null);
    }

    Response put(String path, String body) {
        return send("PUT", path, body);
    }

    Response post(String path, String body) {
        return send("POST", path, body);
    }

    private Response send(String method, String path, String body) {
        try {
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .timeout(Duration.ofSeconds(60))
                    .header("Content-Type", "application/json")
                    .method(method, body == null
                            ? HttpRequest.BodyPublishers.noBody()
                            : HttpRequest.BodyPublishers.ofString(body));
            HttpResponse<String> response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        } catch (Exception e) {
            throw new AssertionError(method + " " + path + " failed", e);
        }
    }
}
