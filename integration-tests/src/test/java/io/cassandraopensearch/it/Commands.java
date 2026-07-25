/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Runs an external command the way an operator would, and never leaves one behind.
 *
 * <p>Output is merged and captured rather than inherited. The ITs assert on what the scripts
 * print — the launcher's "started pid N", the CLI's exit codes and messages — and a test that
 * cannot see them can only report that something exited non-zero.
 */
final class Commands {

    private Commands() {
    }

    /** What a finished command left behind. */
    record Result(List<String> command, int exitCode, String output) {

        boolean succeeded() {
            return exitCode == 0;
        }

        /** Renders the whole invocation for an assertion message. */
        String describe() {
            return String.join(" ", command) + "\n  exit code " + exitCode + "\n"
                    + output.indent(2);
        }
    }

    /**
     * @param timeout hard bound; the process is destroyed and the call fails if it is reached,
     *                because a test that hangs on a wedged node is a test nobody can read
     */
    static Result run(Path workingDirectory, List<String> command, Map<String, String> environment,
                      Duration timeout) {
        ProcessBuilder builder = new ProcessBuilder(command)
                .directory(workingDirectory.toFile())
                .redirectErrorStream(true);
        builder.environment().putAll(environment);

        Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot run " + String.join(" ", command), e);
        }
        try {
            // Draining before waiting, not after: a command that fills the pipe buffer blocks
            // forever otherwise, and `nodetool status` on a busy node is easily a few KiB.
            String output;
            try (InputStream in = process.getInputStream()) {
                output = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                throw new AssertionError(String.join(" ", command) + " did not finish within "
                        + timeout + "\n" + output.indent(2));
            }
            return new Result(command, process.exitValue(), output);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while running " + String.join(" ", command), e);
        } finally {
            process.destroyForcibly();
        }
    }
}
