/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The entry point, exercised through {@code run(args)} rather than {@code main(args)} — the
 * difference between the two is the single {@link System#exit} call in the product, and calling
 * it here would take the test JVM down with it.
 *
 * <p>Exit codes are an interface: the launcher script and the integration tests both branch on
 * them. 2 means "you invoked me wrong or the config is broken, nothing was started"; 1 means
 * "something was started and it failed".
 */
class CassandraOpenSearchServerTest {

    @TempDir
    Path home;

    @Test
    void helpExitsZero() {
        assertThat(CassandraOpenSearchServer.run(new String[]{"--help"})).isZero();
    }

    @Test
    void anUnknownOptionIsARefusalToStart() {
        assertThat(CassandraOpenSearchServer.run(new String[]{"--hoem", "/tmp"})).isEqualTo(2);
    }

    @Test
    void aMissingConfigurationFileIsARefusalToStart() {
        assertThat(CassandraOpenSearchServer.run(new String[]{"--home", home.toString()})).isEqualTo(2);
    }

    @Test
    void aBrokenConfigurationFileIsARefusalToStart() throws Exception {
        writeConfiguration("services:\n  cassandra:\n    jmx_prot: 7199\n  opensearch: {}\n");

        assertThat(CassandraOpenSearchServer.run(new String[]{"--home", home.toString()})).isEqualTo(2);
    }

    @Test
    void aMissingServiceLibraryDirectoryIsAStartupFailure() throws Exception {
        // Nothing is faked here, so this goes through the real ServiceFactory and fails where an
        // incomplete installation would: no lib/cassandra to build the isolated loader from.
        writeConfiguration("services:\n  cassandra: {}\n  opensearch: {}\n");

        assertThat(CassandraOpenSearchServer.run(new String[]{"--home", home.toString()})).isEqualTo(1);
    }

    private void writeConfiguration(String yaml) throws Exception {
        Files.createDirectories(home.resolve("conf"));
        Files.writeString(home.resolve("conf").resolve("cassandra-opensearch.yaml"), yaml);
    }
}
