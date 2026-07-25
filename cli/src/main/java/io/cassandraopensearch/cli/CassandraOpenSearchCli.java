/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.cli;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * {@code bin/cassandra-opensearch status | stop | decommission}.
 *
 * <p>{@code start} is not here: starting the process means choosing a JVM, reading
 * {@code conf/jvm21-server.options} and handing the result to {@code exec}, which is the
 * launcher script's job — see {@code bin/cassandra-opensearch}. Everything this class does
 * happens over JMX against an already-running process.
 *
 * <p>Exit codes are {@code 0} success, {@code 1} the command failed, {@code 2} bad usage,
 * {@code 3} nothing is running. The launcher's {@code -d} start loop polls {@code status} and
 * relies on 3 meaning "not up yet" rather than "broken".
 */
public final class CassandraOpenSearchCli {

    private static final String DEFAULT_JMX_HOST = "127.0.0.1";
    private static final int DEFAULT_JMX_PORT = 7299;

    /**
     * Zero means "use the timeouts in conf/cassandra-opensearch.yaml", which is the right
     * default: {@code shard_relocation_timeout} and {@code ring_streaming_timeout} are sized for
     * how much data this node holds, and the operator typing the command usually knows less
     * about that than the file does.
     */
    private static final Duration DEFAULT_DECOMMISSION_TIMEOUT = Duration.ZERO;

    /** How often the progress watcher polls while the decommission call is blocked. */
    private static final Duration PROGRESS_INTERVAL = Duration.ofSeconds(5);

    private CassandraOpenSearchCli() {
    }

    public static void main(String[] args) {
        try {
            System.exit(run(args));
        } catch (CliException e) {
            System.err.println("error: " + e.getMessage());
            System.exit(e.exitCode());
        } catch (Exception e) {
            System.err.println("error: " + SupervisorMBeanClient.rootCause(e));
            e.printStackTrace(System.err);
            System.exit(CliException.FAILED);
        }
    }

    static int run(String[] args) {
        Deque<String> arguments = new ArrayDeque<>(Arrays.asList(args));
        if (arguments.isEmpty()) {
            usage(System.err);
            return CliException.USAGE;
        }
        String command = arguments.removeFirst();

        String host = System.getenv().getOrDefault("CASSANDRA_OPENSEARCH_JMX_HOST", DEFAULT_JMX_HOST);
        int port = intEnv("CASSANDRA_OPENSEARCH_JMX_PORT", DEFAULT_JMX_PORT);
        String url = null;
        Duration timeout = DEFAULT_DECOMMISSION_TIMEOUT;
        boolean force = false;
        boolean quiet = false;

        while (!arguments.isEmpty()) {
            String option = arguments.removeFirst();
            switch (option) {
                case "--jmx-host" -> host = value(arguments, option);
                case "--jmx-port" -> port = Integer.parseInt(value(arguments, option));
                case "--jmx-url" -> url = value(arguments, option);
                case "--timeout" -> timeout = parseDuration(value(arguments, option));
                case "--force" -> force = true;
                case "--quiet", "-q" -> quiet = true;
                case "--help", "-h" -> {
                    usage(System.out);
                    return 0;
                }
                default -> throw new CliException(CliException.USAGE, "unknown option: " + option);
            }
        }

        String serviceUrl = url != null ? url
                : "service:jmx:rmi:///jndi/rmi://" + host + ':' + port + "/jmxrmi";

        switch (command) {
            case "status" -> {
                return status(serviceUrl, quiet);
            }
            case "stop" -> {
                return stop(serviceUrl, quiet);
            }
            case "decommission" -> {
                return decommission(serviceUrl, timeout, force);
            }
            case "help", "--help", "-h" -> {
                usage(System.out);
                return 0;
            }
            default -> {
                System.err.println("error: unknown command: " + command);
                usage(System.err);
                return CliException.USAGE;
            }
        }
    }

    private static int status(String serviceUrl, boolean quiet) {
        try (SupervisorMBeanClient client = SupervisorMBeanClient.connect(serviceUrl)) {
            if (quiet) {
                return 0;
            }
            System.out.println(client.objectName() + "  (" + client.serviceUrl() + ')');
            Map<String, Object> attributes = client.attributes();
            if (attributes.isEmpty()) {
                System.out.println("  the supervisor MBean exposes no attributes");
                return 0;
            }
            int width = attributes.keySet().stream().mapToInt(String::length).max().orElse(0);
            for (Map.Entry<String, Object> attribute : attributes.entrySet()) {
                System.out.printf("  %-" + width + "s : %s%n", attribute.getKey(),
                        format(attribute.getValue()));
            }
            // The per-service detail is where the useful part is — ring state, shard counts,
            // listen addresses — and it is an operation rather than an attribute, so the generic
            // dump above cannot reach it.
            for (String service : serviceNames(attributes)) {
                Map<String, String> details = client.serviceDetails(service);
                if (details.isEmpty()) {
                    continue;
                }
                System.out.println();
                System.out.println("  " + service);
                int detailWidth = details.keySet().stream().mapToInt(String::length).max().orElse(0);
                for (Map.Entry<String, String> detail : details.entrySet()) {
                    System.out.printf("    %-" + detailWidth + "s : %s%n",
                            detail.getKey(), detail.getValue());
                }
            }
            return 0;
        }
    }

    private static List<String> serviceNames(Map<String, Object> attributes) {
        Object names = attributes.get("ServiceNames");
        if (names instanceof String[] array) {
            return List.of(array);
        }
        if (names instanceof Object[] array) {
            return Arrays.stream(array).map(String::valueOf).toList();
        }
        return List.of();
    }

    private static int stop(String serviceUrl, boolean quiet) {
        try (SupervisorMBeanClient client = SupervisorMBeanClient.connect(serviceUrl)) {
            if (!quiet) {
                System.out.println("stopping " + client.objectName() + " ...");
            }
            client.stop();
        } catch (CliException e) {
            if (e.exitCode() == CliException.NO_SUCH_OPERATION) {
                // Not a defect: the supervisor owns the JVM's only shutdown hook, so SIGTERM is
                // the ordered-teardown path. bin/cassandra-opensearch branches on exit code 4
                // and sends the signal; say so, for anyone who ran this command directly.
                System.err.println("the supervisor MBean exposes no stop operation; send SIGTERM"
                        + " to the process instead (bin/cassandra-opensearch stop does this for"
                        + " you when it can read the pid file)");
                return CliException.NO_SUCH_OPERATION;
            }
            // The connection dying underneath a stop is the expected outcome, not a failure: the
            // supervisor tears down its own JMX connector on the way out, so the RMI call often
            // never gets to return.
            if (e.exitCode() == CliException.FAILED && isConnectionLoss(e)) {
                if (!quiet) {
                    System.out.println("stop requested; the process closed the connection while shutting down");
                }
                return 0;
            }
            throw e;
        }
        if (!quiet) {
            System.out.println("stop requested");
        }
        return 0;
    }

    private static int decommission(String serviceUrl, Duration timeout, boolean force) {
        try (SupervisorMBeanClient client = SupervisorMBeanClient.connect(serviceUrl)) {
            System.out.println("decommissioning " + client.objectName()
                    + (timeout.isZero() ? " (timeouts from conf/cassandra-opensearch.yaml"
                                        : " (timeout " + timeout)
                    + (force ? ", forced" : "") + ") ...");
            System.out.println("this relocates every OpenSearch shard off this node before"
                    + " Cassandra leaves the ring, and can legitimately take an hour");
            Thread watcher = startProgressWatcher(serviceUrl);
            try {
                // Blocks for the whole procedure; the watcher above is on its own connection
                // precisely so that something can be printed while it does.
                System.out.println(client.decommission(timeout.toSeconds(), force));
            } finally {
                watcher.interrupt();
            }
            return 0;
        } catch (CliException e) {
            if (e.exitCode() == CliException.FAILED && isConnectionLoss(e)) {
                // Step 7 of the decommission sequence is `exit 0`; losing the connection here
                // means it got that far.
                System.out.println("the process closed the connection, which is what a completed"
                        + " decommission looks like — check the log to confirm");
                return 0;
            }
            throw e;
        }
    }

    /**
     * Follows {@code DecommissionProgress} on a second connection and prints it as it changes.
     *
     * <p>A daemon thread with its own connector: the JMX call carrying the decommission is
     * blocked for the duration, and an RMI connection cannot carry a second call while it is.
     * Failures here are swallowed — losing sight of the progress is not a reason to fail a
     * decommission that is otherwise going fine.
     */
    private static Thread startProgressWatcher(String serviceUrl) {
        Thread watcher = new Thread(() -> {
            String last = null;
            try (SupervisorMBeanClient client = SupervisorMBeanClient.connect(serviceUrl)) {
                while (!Thread.currentThread().isInterrupted()) {
                    String progress = client.decommissionProgress();
                    if (progress != null && !progress.isBlank() && !progress.equals(last)) {
                        last = progress;
                        System.out.println("  " + progress);
                    }
                    Thread.sleep(PROGRESS_INTERVAL.toMillis());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                // No progress reporting; the decommission itself is unaffected.
            }
        }, "decommission-progress");
        watcher.setDaemon(true);
        watcher.start();
        return watcher;
    }

    private static boolean isConnectionLoss(Throwable failure) {
        for (Throwable cause = failure; cause != null && cause != cause.getCause(); cause = cause.getCause()) {
            if (cause instanceof java.io.IOException || cause instanceof java.rmi.RemoteException) {
                return true;
            }
        }
        return false;
    }

    private static String format(Object value) {
        if (value instanceof Object[] array) {
            return Arrays.toString(array);
        }
        if (value instanceof Map<?, ?> map) {
            StringBuilder rendered = new StringBuilder();
            map.forEach((key, entry) -> rendered.append(rendered.isEmpty() ? "" : ", ")
                    .append(key).append('=').append(entry));
            return rendered.toString();
        }
        if (value instanceof javax.management.openmbean.TabularData tabular) {
            StringBuilder rendered = new StringBuilder();
            for (Object row : tabular.values()) {
                if (row instanceof javax.management.openmbean.CompositeData composite) {
                    List<String> keys = List.copyOf(composite.getCompositeType().keySet());
                    for (String key : keys) {
                        rendered.append(rendered.isEmpty() ? "" : ", ")
                                .append(key).append('=').append(composite.get(key));
                    }
                }
            }
            return rendered.toString();
        }
        return String.valueOf(value);
    }

    private static String value(Deque<String> arguments, String option) {
        if (arguments.isEmpty()) {
            throw new CliException(CliException.USAGE, option + " needs a value");
        }
        return arguments.removeFirst();
    }

    private static int intEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new CliException(CliException.USAGE, name + " is not a number: " + value);
        }
    }

    /** Accepts {@code 90s}, {@code 30m}, {@code 2h} and a bare number of seconds. */
    static Duration parseDuration(String text) {
        String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            throw new CliException(CliException.USAGE, "empty duration");
        }
        char unit = trimmed.charAt(trimmed.length() - 1);
        String number = Character.isDigit(unit) ? trimmed : trimmed.substring(0, trimmed.length() - 1);
        long amount;
        try {
            amount = Long.parseLong(number.trim());
        } catch (NumberFormatException e) {
            throw new CliException(CliException.USAGE,
                    "cannot read '" + text + "' as a duration; use 90s, 30m, 2h or a number of seconds");
        }
        return switch (Character.isDigit(unit) ? 's' : unit) {
            case 's' -> Duration.ofSeconds(amount);
            case 'm' -> Duration.ofMinutes(amount);
            case 'h' -> Duration.ofHours(amount);
            case 'd' -> Duration.ofDays(amount);
            default -> throw new CliException(CliException.USAGE,
                    "unknown duration unit '" + unit + "' in '" + text + "'; use s, m, h or d");
        };
    }

    private static void usage(java.io.PrintStream out) {
        out.println("""
                usage: cassandra-opensearch <command> [options]

                commands:
                  start [-d] [-p <pidfile>]  run the node; -d detaches (handled by the launcher script)
                  status                     print what the supervisor reports about the process
                  stop                       ask the supervisor to shut both services down in order
                  decommission               hand this node's data over and leave both clusters

                options:
                  --jmx-host <host>   supervisor JMX host (default 127.0.0.1, or CASSANDRA_OPENSEARCH_JMX_HOST)
                  --jmx-port <port>   supervisor JMX port (default 7299, or CASSANDRA_OPENSEARCH_JMX_PORT)
                  --jmx-url <url>     full service:jmx: URL, instead of host and port
                  --timeout <90s|30m> bound on each waiting phase of a decommission; the default
                                      uses the limits in conf/cassandra-opensearch.yaml
                  --force             decommission even with shards or ranges still outstanding;
                                      anything on this node without another copy is lost
                  -q, --quiet         no output, exit code only

                exit codes: 0 ok, 1 failed, 2 bad usage, 3 nothing running,
                            4 the MBean has no such operation""");
    }
}
