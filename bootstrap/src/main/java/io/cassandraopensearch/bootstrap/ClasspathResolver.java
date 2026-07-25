/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.bootstrap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
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

    private static final Logger LOG = LoggerFactory.getLogger(ClasspathResolver.class);

    private static final String JAR_SUFFIX = ".jar";

    private ClasspathResolver() {
    }

    /**
     * Lists every {@code .jar} in {@code libDirectory}, sorted by file name.
     *
     * <p>The sort is not cosmetic: classpath order decides which copy wins when two jars contain
     * the same class, so an unsorted directory listing would make startup depend on filesystem
     * iteration order and produce failures that reproduce on one machine but not another. It is
     * also why the listing is collected with {@code toList()} rather than drained with
     * {@code forEach}, which is explicitly not required to preserve encounter order.
     *
     * <p>The suffix match is case-insensitive, and everything skipped is logged at WARN. A jar
     * dropped from a service's classpath does not fail here; it fails much later, as a
     * {@code NoClassDefFoundError} deep inside a server's startup, and an operator who has just
     * copied {@code E-Upper.JAR} into {@code lib/} has no way to connect the two. A broken
     * symlink is the same story: {@link Files#isRegularFile} follows links, so a dangling one is
     * indistinguishable from an absent file unless it is reported.
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
        List<Path> entries;
        try (Stream<Path> listing = Files.list(libDirectory)) {
            entries = listing
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot list " + libDirectory.toAbsolutePath(), e);
        }
        List<URL> jars = new ArrayList<>();
        for (Path entry : entries) {
            String name = entry.getFileName().toString();
            if (!name.toLowerCase(Locale.ROOT).endsWith(JAR_SUFFIX)) {
                LOG.warn("Skipping {}: not a {} file, so it is not on the classpath of {}",
                        entry.toAbsolutePath(), JAR_SUFFIX, libDirectory.getFileName());
                continue;
            }
            if (!Files.isRegularFile(entry)) {
                LOG.warn("Skipping {}: names a {} but is not a regular file — a broken symlink, or"
                                + " a directory. It will NOT be on the classpath of {}.",
                        entry.toAbsolutePath(), JAR_SUFFIX, libDirectory.getFileName());
                continue;
            }
            jars.add(toUrl(entry));
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
