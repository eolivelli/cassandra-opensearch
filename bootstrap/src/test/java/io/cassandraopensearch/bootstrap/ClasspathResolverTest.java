/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * What a service's classpath is built from. Every defect this pins has the same shape: a jar the
 * operator put in {@code lib/<service>/} does not reach the classpath, nothing says so, and the
 * consequence surfaces much later as a {@code NoClassDefFoundError} from inside a server's
 * startup that has no visible connection to the jar that is missing.
 */
class ClasspathResolverTest {

    @Test
    void listsJarsSortedByFileName(@TempDir Path lib) throws IOException {
        touch(lib, "c.jar");
        touch(lib, "a.jar");
        touch(lib, "b.jar");

        // The order is load-bearing: it decides which copy wins when two jars hold the same
        // class, so an unsorted listing makes startup depend on filesystem iteration order.
        assertThat(fileNames(ClasspathResolver.jarsIn(lib)))
                .containsExactly("a.jar", "b.jar", "c.jar");
    }

    /**
     * {@code endsWith(".jar")} is case-sensitive, so {@code E-Upper.JAR} was dropped from the
     * classpath without a word. Case is not the operator's choice — it is whatever the artifact
     * was published with.
     */
    @Test
    void jarSuffixIsMatchedWithoutRegardToCase(@TempDir Path lib) throws IOException {
        touch(lib, "lower.jar");
        touch(lib, "E-Upper.JAR");
        touch(lib, "Mixed.Jar");

        assertThat(fileNames(ClasspathResolver.jarsIn(lib)))
                .containsExactlyInAnyOrder("lower.jar", "E-Upper.JAR", "Mixed.Jar");
    }

    /**
     * {@link Files#isRegularFile} follows symlinks, so a dangling one is indistinguishable from a
     * file that was never there. Skipping it is right — its URL would fail at class load — but
     * skipping it silently is what leaves the operator with no way back to the cause.
     */
    @Test
    void aBrokenSymlinkIsSkippedRatherThanPutOnTheClasspath(@TempDir Path lib) throws IOException {
        touch(lib, "real.jar");
        try {
            Files.createSymbolicLink(lib.resolve("dangling.jar"), lib.resolve("gone.jar"));
        } catch (UnsupportedOperationException | FileSystemException e) {
            assumeThat(false).as("this filesystem does not support symlinks").isTrue();
        }

        assertThat(fileNames(ClasspathResolver.jarsIn(lib))).containsExactly("real.jar");
    }

    @Test
    void directoriesAndNonJarFilesAreSkipped(@TempDir Path lib) throws IOException {
        touch(lib, "real.jar");
        touch(lib, "README.txt");
        touch(lib, "notes.jar.bak");
        Files.createDirectory(lib.resolve("nested.jar"));

        assertThat(fileNames(ClasspathResolver.jarsIn(lib))).containsExactly("real.jar");
    }

    @Test
    void everyJarBecomesAUsableUrl(@TempDir Path lib) throws IOException {
        touch(lib, "only.jar");

        List<URL> jars = ClasspathResolver.jarsIn(lib);

        assertThat(jars).hasSize(1);
        assertThat(jars.get(0).getProtocol()).isEqualTo("file");
        assertThat(Path.of(jars.get(0).getPath())).isEqualTo(lib.resolve("only.jar"));
    }

    @Test
    void refusesADirectoryThatDoesNotExist(@TempDir Path lib) {
        assertThatThrownBy(() -> ClasspathResolver.jarsIn(lib.resolve("absent")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Not a directory");
    }

    @Test
    void refusesAFileWhereADirectoryWasExpected(@TempDir Path lib) throws IOException {
        Path file = touch(lib, "lib-is-a-file");

        assertThatThrownBy(() -> ClasspathResolver.jarsIn(file))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Not a directory");
    }

    /**
     * An empty classpath cannot work: nothing falls back to the supervisor's classpath by design,
     * so this has to fail here rather than as a mystery inside the loader.
     */
    @Test
    void refusesADirectoryWithNoJars(@TempDir Path lib) throws IOException {
        touch(lib, "README.txt");

        assertThatThrownBy(() -> ClasspathResolver.jarsIn(lib))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No jars found");
    }

    private static Path touch(Path directory, String name) throws IOException {
        return Files.writeString(directory.resolve(name), "");
    }

    private static List<String> fileNames(List<URL> jars) {
        return jars.stream().map(url -> Path.of(url.getPath()).getFileName().toString()).toList();
    }
}
