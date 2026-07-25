/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.cli;

/**
 * A failure with an exit code an operator's script can branch on.
 *
 * <p>The distinction that matters is {@link #NOT_RUNNING} versus {@link #FAILED}: "there is
 * nothing listening" is the normal answer to {@code status} on a stopped node, and a wrapper
 * script polling for startup has to be able to tell it apart from "the process is there and
 * something went wrong".
 */
final class CliException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Bad arguments; the usage message has been printed. */
    static final int USAGE = 2;
    /** The command could not be carried out. */
    static final int FAILED = 1;
    /** Nothing is listening on the supervisor's JMX port. */
    static final int NOT_RUNNING = 3;

    private final int exitCode;

    CliException(int exitCode, String message) {
        super(message);
        this.exitCode = exitCode;
    }

    CliException(int exitCode, String message, Throwable cause) {
        super(message, cause);
        this.exitCode = exitCode;
    }

    int exitCode() {
        return exitCode;
    }
}
