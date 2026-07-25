/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Parses the {@code cassandra-opensearch.yaml} that the distribution actually ships.
 *
 * <p>The unit tests above all build their YAML inline, so they would happily keep passing while
 * the file every installation starts from had drifted into something the parser rejects — a
 * failure the operator would meet on first boot. This closes that gap early; the integration
 * tests check it again against the assembled tarball.
 */
class ShippedConfigurationTest {

    @TempDir
    Path home;

    private static final Path SHIPPED =
            Paths.get("..", "dist", "src", "main", "resources", "conf", "cassandra-opensearch.yaml");

    @Test
    void theDefaultConfigurationFileParsesAndCarriesTheDocumentedDefaults() throws Exception {
        assumeTrue(Files.isRegularFile(SHIPPED), "not running from the source tree");

        Files.createDirectories(home.resolve("conf"));
        Files.copy(SHIPPED, home.resolve("conf").resolve("cassandra-opensearch.yaml"));

        NodeConfiguration configuration = NodeConfiguration.load(home);

        // The ports the README and the CLI wrappers tell users to connect to. Cassandra's native
        // transport port is not among them: it is declared here for the operator's benefit but
        // Cassandra reads it from conf/cassandra.yaml, so it never reaches the runtime.
        assertThat(configuration.service("cassandra").settings())
                .containsEntry("jmx.port", "7199");
        assertThat(configuration.service("opensearch").settings())
                .containsEntry("http_port", "9200");
        assertThat(configuration.service("cassandra").enabled()).isTrue();
        assertThat(configuration.service("opensearch").enabled()).isTrue();
    }
}
