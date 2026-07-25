/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.cli;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The parts of the CLI that do not need a running node: duration parsing, argument parsing and
 * the exit-code contract.
 *
 * <p>The exit codes are the interface, not an implementation detail. {@code
 * bin/cassandra-opensearch} branches on 4 to fall back to SIGTERM, its {@code start -d} loop
 * relies on 3 meaning "not up yet" rather than "broken", and an operator's own script has nothing
 * else to go on. Every assertion below that names a number is naming that contract.
 *
 * <p>Nothing here connects to a real supervisor — the integration tests do that. What these can
 * do, and the ITs cannot cheaply, is drive the failure paths: a closed port, a bad option, a
 * duration nobody can read.
 */
class CassandraOpenSearchCliTest {

    /**
     * A port that is bound and immediately closed, so nothing is listening on it for the rest of
     * the test run. A hard-coded port would collide with whatever the developer happens to be
     * running; port 0 lets the kernel pick one that is free.
     */
    private static int closedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private ByteArrayOutputStream out;
    private ByteArrayOutputStream err;
    private PrintStream originalOut;
    private PrintStream originalErr;

    @BeforeEach
    void captureStreams() {
        originalOut = System.out;
        originalErr = System.err;
        out = new ByteArrayOutputStream();
        err = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void restoreStreams() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    private String stdout() {
        return out.toString(StandardCharsets.UTF_8);
    }

    private String stderr() {
        return err.toString(StandardCharsets.UTF_8);
    }

    // --- durations -------------------------------------------------------------------------

    @Nested
    class ParseDuration {

        @ParameterizedTest
        @CsvSource({
                "90s, PT1M30S",
                "30m, PT30M",
                "2h, PT2H",
                "1d, PT24H",
                "45,  PT45S",
                "0,   PT0S",
                "0s,  PT0S",
        })
        void readsTheDocumentedForms(String text, String expected) {
            assertThat(CassandraOpenSearchCli.parseDuration(text)).isEqualTo(Duration.parse(expected));
        }

        /** A bare number is seconds; that is what the usage message promises. */
        @Test
        void aBareNumberIsSeconds() {
            assertThat(CassandraOpenSearchCli.parseDuration("300")).isEqualTo(Duration.ofMinutes(5));
        }

        @Test
        void tolerantOfSurroundingWhitespace() {
            assertThat(CassandraOpenSearchCli.parseDuration("  30m  ")).isEqualTo(Duration.ofMinutes(30));
        }

        /**
         * Every rejection is usage (2), never failure (1). A mistyped {@code --timeout} has not
         * failed a decommission — it has not started one, and the two have to be distinguishable
         * to anything scripting this.
         */
        @ParameterizedTest
        @ValueSource(strings = {"", "   ", "abc", "m", "10x", "1 0s", "s90"})
        void rejectsWhatItCannotRead(String text) {
            assertThatThrownBy(() -> CassandraOpenSearchCli.parseDuration(text))
                    .isInstanceOf(CliException.class)
                    .extracting(failure -> ((CliException) failure).exitCode())
                    .isEqualTo(CliException.USAGE);
        }

        /** The message has to name the offending text and the forms that would have worked. */
        @Test
        void saysWhatWouldHaveWorked() {
            assertThatThrownBy(() -> CassandraOpenSearchCli.parseDuration("soon"))
                    .hasMessageContaining("soon")
                    .hasMessageContaining("90s");
            assertThatThrownBy(() -> CassandraOpenSearchCli.parseDuration("10y"))
                    .hasMessageContaining("10y")
                    .hasMessageContaining("s, m, h or d");
        }
    }

    // --- argument parsing ------------------------------------------------------------------

    @Nested
    class ArgumentParsing {

        @Test
        void noArgumentsIsUsageAndPrintsIt() {
            assertThat(CassandraOpenSearchCli.run(new String[0])).isEqualTo(CliException.USAGE);
            assertThat(stderr()).contains("usage: cassandra-opensearch");
        }

        @Test
        void anUnknownCommandIsUsageAndNamesTheCommand() {
            assertThat(CassandraOpenSearchCli.run(new String[] {"frobnicate"}))
                    .isEqualTo(CliException.USAGE);
            assertThat(stderr()).contains("unknown command: frobnicate");
        }

        @Test
        void anUnknownOptionIsUsageAndNamesTheOption() {
            assertThatThrownBy(() -> CassandraOpenSearchCli.run(new String[] {"status", "--wat"}))
                    .isInstanceOf(CliException.class)
                    .hasMessageContaining("--wat")
                    .extracting(failure -> ((CliException) failure).exitCode())
                    .isEqualTo(CliException.USAGE);
        }

        @ParameterizedTest
        @ValueSource(strings = {"--jmx-host", "--jmx-port", "--jmx-url", "--timeout"})
        void anOptionWithNoValueIsUsage(String option) {
            assertThatThrownBy(() -> CassandraOpenSearchCli.run(new String[] {"status", option}))
                    .isInstanceOf(CliException.class)
                    .hasMessageContaining(option + " needs a value")
                    .extracting(failure -> ((CliException) failure).exitCode())
                    .isEqualTo(CliException.USAGE);
        }

        /**
         * A typo in {@code --jmx-port} is usage (2), not failure (1). It used to escape as a raw
         * {@link NumberFormatException}, which the generic handler in {@code main} turns into
         * exit 1 plus a stack trace — the code a script reads as "the node is broken".
         */
        @ParameterizedTest
        @ValueSource(strings = {"seven-two-nine-nine", "", "7299x", "0", "-1", "65536", "99999999999"})
        void aBadPortIsUsageRatherThanFailure(String port) {
            assertThatThrownBy(() -> CassandraOpenSearchCli.parsePort(port))
                    .isInstanceOf(CliException.class)
                    .hasMessageContaining("--jmx-port")
                    .extracting(failure -> ((CliException) failure).exitCode())
                    .isEqualTo(CliException.USAGE);
        }

        @Test
        void acceptsAPortInRange() {
            assertThat(CassandraOpenSearchCli.parsePort("7299")).isEqualTo(7299);
            assertThat(CassandraOpenSearchCli.parsePort(" 21014 ")).isEqualTo(21014);
            assertThat(CassandraOpenSearchCli.parsePort("65535")).isEqualTo(65535);
        }

        @Test
        void helpIsSuccessOnStdout() {
            assertThat(CassandraOpenSearchCli.run(new String[] {"help"})).isZero();
            assertThat(stdout()).contains("usage: cassandra-opensearch", "exit codes:");
            assertThat(stderr()).isEmpty();
        }

        @ParameterizedTest
        @ValueSource(strings = {"--help", "-h"})
        void helpIsAlsoACommandAndAnOption(String form) {
            assertThat(CassandraOpenSearchCli.run(new String[] {form})).isZero();
            assertThat(CassandraOpenSearchCli.run(new String[] {"status", form})).isZero();
            assertThat(stdout()).contains("usage: cassandra-opensearch");
        }

        /** The usage text is the only documentation of the exit codes an operator sees. */
        @Test
        void usageDocumentsEveryExitCode() {
            CassandraOpenSearchCli.run(new String[] {"help"});
            assertThat(stdout())
                    .contains("0 ok")
                    .contains("1 failed")
                    .contains("2 bad usage")
                    .contains("3 nothing running")
                    .contains("4 the MBean has no such operation");
        }
    }

    // --- exit codes ------------------------------------------------------------------------

    @Nested
    class ExitCodes {

        /**
         * The numbers themselves, pinned. {@code bin/cassandra-opensearch} tests {@code -eq 4}
         * literally, and changing any of these silently changes the launcher's behaviour.
         */
        @Test
        void areTheDocumentedNumbers() {
            assertThat(CliException.FAILED).isEqualTo(1);
            assertThat(CliException.USAGE).isEqualTo(2);
            assertThat(CliException.NOT_RUNNING).isEqualTo(3);
            assertThat(CliException.NO_SUCH_OPERATION).isEqualTo(4);
        }

        /**
         * Nothing listening is 3, for every command that has to connect — not 1. The launcher's
         * {@code start -d} loop polls {@code status} and depends on that distinction: 3 means
         * "not up yet", anything else means stop waiting.
         */
        @ParameterizedTest
        @ValueSource(strings = {"status", "stop", "decommission"})
        void nothingListeningIsNotRunning(String command) throws IOException {
            String port = String.valueOf(closedPort());

            assertThatThrownBy(() -> CassandraOpenSearchCli.run(
                    new String[] {command, "--jmx-host", "127.0.0.1", "--jmx-port", port}))
                    .isInstanceOf(CliException.class)
                    .hasMessageContaining("Cannot reach a cassandra-opensearch process")
                    .extracting(failure -> ((CliException) failure).exitCode())
                    .isEqualTo(CliException.NOT_RUNNING);
        }

        /** The message has to point at the port, because a wrong port is the usual cause. */
        @Test
        void theNotRunningMessageNamesTheEndpointAndTheLikelyCause() throws IOException {
            int port = closedPort();

            assertThatThrownBy(() -> CassandraOpenSearchCli.run(
                    new String[] {"status", "--jmx-port", String.valueOf(port)}))
                    .hasMessageContaining("127.0.0.1:" + port)
                    .hasMessageContaining("CASSANDRA_OPENSEARCH_JMX_PORT");
        }

        /** {@code --jmx-url} replaces host and port rather than being merged with them. */
        @Test
        void anExplicitServiceUrlIsUsedVerbatim() throws IOException {
            int port = closedPort();
            String url = "service:jmx:rmi:///jndi/rmi://127.0.0.1:" + port + "/jmxrmi";

            assertThatThrownBy(() -> CassandraOpenSearchCli.run(
                    new String[] {"status", "--jmx-host", "10.9.9.9", "--jmx-port", "1", "--jmx-url", url}))
                    .hasMessageContaining(url)
                    .hasMessageNotContaining("10.9.9.9");
        }

        /** {@code --quiet} is the launcher's polling form: exit code only, nothing on stdout. */
        @Test
        void quietStatusPrintsNothing() throws IOException {
            String port = String.valueOf(closedPort());

            assertThatThrownBy(() -> CassandraOpenSearchCli.run(
                    new String[] {"status", "--quiet", "--jmx-port", port}))
                    .isInstanceOf(CliException.class);
            assertThat(stdout()).isEmpty();
        }
    }

    // --- the connection-loss rule ------------------------------------------------------------

    /**
     * {@code isConnectionLoss} is what makes a clean {@code stop} report success. The supervisor
     * closes its own JMX connector while shutting down, so the RMI call carrying the stop usually
     * dies rather than returning — and the CLI has to read that as "it did what I asked".
     */
    @Nested
    class ConnectionLoss {

        @Test
        void anIoExceptionAnywhereInTheChainCounts() {
            assertThat(CassandraOpenSearchCli.isConnectionLoss(new IOException("closed"))).isTrue();
            assertThat(CassandraOpenSearchCli.isConnectionLoss(
                    new IllegalStateException("wrapped", new IOException("closed")))).isTrue();
            assertThat(CassandraOpenSearchCli.isConnectionLoss(new CliException(
                    CliException.FAILED, "stop failed",
                    new RuntimeException(new ConnectException("connection refused"))))).isTrue();
        }

        @Test
        void soDoesARemoteException() {
            assertThat(CassandraOpenSearchCli.isConnectionLoss(new RemoteException("gone"))).isTrue();
        }

        /**
         * And nothing else does. A supervisor that answered and then refused the operation is a
         * failure the operator has to see, not a stop that quietly "worked".
         */
        @Test
        void anythingElseIsAFailure() {
            assertThat(CassandraOpenSearchCli.isConnectionLoss(
                    new IllegalStateException("the node refused"))).isFalse();
            assertThat(CassandraOpenSearchCli.isConnectionLoss(new CliException(
                    CliException.FAILED, "stop failed",
                    new SecurityException("access denied")))).isFalse();
        }

        /** A cause chain that points at itself must not spin forever. */
        @Test
        void toleratesASelfReferencingCause() {
            RuntimeException loop = new RuntimeException("round and round") {
                private static final long serialVersionUID = 1L;

                @Override
                public synchronized Throwable getCause() {
                    return this;
                }
            };
            assertThat(CassandraOpenSearchCli.isConnectionLoss(loop)).isFalse();
        }
    }
}
