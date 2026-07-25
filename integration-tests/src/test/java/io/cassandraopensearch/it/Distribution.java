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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The built tarball, and how to get an installation out of it.
 *
 * <p>The ITs drive the archive rather than the reactor's class files because the archive is the
 * product. Everything that can go wrong between a green {@code mvn install} and a node an
 * operator can start — a jar that was not collected, a script that lost its executable bit, a
 * classpath directory that resolves against the wrong root — is invisible to a test that builds
 * its own classpath.
 */
final class Distribution {

    /** Set by failsafe; see integration-tests/pom.xml. */
    private static final String ARCHIVE_PROPERTY = "dist.archive";

    private Distribution() {
    }

    static Path archive() {
        String configured = System.getProperty(ARCHIVE_PROPERTY);
        if (configured == null || configured.isBlank()) {
            throw new IllegalStateException(
                    "-D" + ARCHIVE_PROPERTY + " is not set. These tests run under failsafe, which"
                            + " points it at dist/tarball/target/*-bin.tar.gz.");
        }
        Path archive = Path.of(configured);
        if (!Files.isRegularFile(archive)) {
            throw new IllegalStateException(
                    "no tarball at " + archive + ". Run `mvn install` first: the ITs drive the"
                            + " assembled distribution, not the reactor's target/classes.");
        }
        return archive;
    }

    /**
     * Unpacks the archive into {@code directory}, which is emptied first.
     *
     * @return the installation root — the archive's single top-level directory
     */
    static Path unpack(Path directory) {
        deleteRecursively(directory);
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // The system tar rather than a Java implementation: the archive carries the file modes
        // that make bin/* executable, and reproducing that faithfully is exactly the detail a
        // hand-rolled extractor gets wrong.
        Commands.run(directory, List.of("tar", "-xzf", archive().toString(), "-C", directory.toString()),
                Map.of(), Duration.ofMinutes(2));
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.filter(Files::isDirectory).findFirst().orElseThrow(
                    () -> new IllegalStateException("the archive has no top-level directory"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Extracts one file from the archive without unpacking the other 118 MB.
     *
     * @param relativePath path below the installation root, e.g. {@code conf/opensearch.yml}
     * @return the extracted file, exactly as shipped
     */
    static Path extract(String relativePath, Path into) {
        try {
            Files.createDirectories(into);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Commands.run(into, List.of("tar", "-xzf", archive().toString(), "-C", into.toString(),
                        "--strip-components=1", "--wildcards", "*/" + relativePath),
                Map.of(), Duration.ofMinutes(2));
        Path extracted = into.resolve(relativePath);
        if (!Files.isRegularFile(extracted)) {
            throw new IllegalStateException("the archive has no " + relativePath);
        }
        return extracted;
    }

    /**
     * Where one test class's node is installed.
     *
     * <p>Under {@code target/} rather than in a temporary directory that the framework deletes:
     * when an IT fails, the node's data files, its rewritten configuration and its logs are the
     * evidence, and they are worth more than the disk they occupy until the next {@code clean}.
     */
    static Path workDirectory(Class<?> testClass, String node) {
        return Path.of(System.getProperty("it.work.directory", "target/it-nodes"))
                .resolve(testClass.getSimpleName())
                .resolve(node)
                .toAbsolutePath();
    }

    static void deleteRecursively(Path directory) {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> entries = Files.walk(directory)) {
            entries.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
