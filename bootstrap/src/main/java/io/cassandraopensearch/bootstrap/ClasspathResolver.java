/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.bootstrap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Turns a {@code lib/<service>/} directory from the distribution into the classpath for that
 * service's {@link IsolatedClassLoader}.
 *
 * <p>The distribution keeps each service's jars in its own directory precisely so that this can
 * be a directory listing rather than a curated list — adding a Cassandra dependency means
 * dropping a jar in {@code lib/cassandra/}, with no code change here.
 */
public final class ClasspathResolver {

    private ClasspathResolver() {
    }

    /**
     * Lists every {@code .jar} in {@code libDirectory}, sorted by file name.
     *
     * <p>The sort is not cosmetic: classpath order decides which copy wins when two jars contain
     * the same class, so an unsorted directory listing would make startup depend on filesystem
     * iteration order and produce failures that reproduce on one machine but not another.
     *
     * @param libDirectory a directory such as {@code <home>/lib/cassandra}
     * @return the jars, in a stable order; never empty
     * @throws IllegalArgumentException if the directory is missing, is not a directory, or holds no jars
     */
    public static List<URL> jarsIn(Path libDirectory) {
        if (!Files.isDirectory(libDirectory)) {
            throw new IllegalArgumentException(
                    "Not a directory: " + libDirectory.toAbsolutePath()
                            + ". The distribution must provide one lib/<service> directory per"
                            + " isolated service; see docs/ARCHITECTURE.md.");
        }
        List<URL> jars = new ArrayList<>();
        try (Stream<Path> entries = Files.list(libDirectory)) {
            entries.filter(p -> p.getFileName().toString().endsWith(".jar"))
                    .filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .forEach(p -> jars.add(toUrl(p)));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot list " + libDirectory.toAbsolutePath(), e);
        }
        if (jars.isEmpty()) {
            throw new IllegalArgumentException(
                    "No jars found in " + libDirectory.toAbsolutePath()
                            + ". The service cannot start with an empty classpath: nothing falls"
                            + " back to the supervisor's classpath by design.");
        }
        return List.copyOf(jars);
    }

    private static URL toUrl(Path path) {
        try {
            return path.toUri().toURL();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot convert to URL: " + path, e);
        }
    }
}
