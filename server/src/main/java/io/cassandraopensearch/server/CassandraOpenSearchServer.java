/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.server.config.ConfigurationException;
import io.cassandraopensearch.server.config.NodeConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The process. {@code bin/cassandra-opensearch start} runs this class and nothing else.
 *
 * <p>Its whole job is to turn a command line into a {@link NodeConfiguration}, hand that to a
 * {@link Supervisor}, and block until the supervisor says the process should end. The exit code
 * comes back from {@link Supervisor#awaitShutdown()}, and {@link #main} is the only place in the
 * product that calls {@link System#exit} — everything below it has to be able to run inside a
 * test, and inside a shutdown hook, without taking the JVM with it.
 *
 * <p>Note what is <b>not</b> here: no {@code io.netty.*} or {@code jna.*} system property. Both
 * embedded servers read those out of the same JVM globals, so a value set for one silently
 * applies to the other — {@code io.netty.noUnsafe=true}, which OpenSearch's own launcher sets,
 * measurably slows Cassandra's native transport. Anything that genuinely must be set belongs on
 * the launcher's command line, where the two can be reconciled once and reviewed.
 */
public final class CassandraOpenSearchServer {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraOpenSearchServer.class);

    /** Set by the launcher script, which knows where it was installed. */
    private static final String HOME_PROPERTY = "cassandra-opensearch.home";
    private static final String CONFIG_PROPERTY = "cassandra-opensearch.config";
    private static final String HOME_ENVIRONMENT = "CASSANDRA_OPENSEARCH_HOME";

    private static final int EXIT_OK = 0;
    private static final int EXIT_FAILED = 1;
    private static final int EXIT_BAD_INVOCATION = 2;

    private CassandraOpenSearchServer() {
    }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    /**
     * The body of {@link #main}, minus the exit.
     *
     * @return the process exit code
     */
    static int run(String[] args) {
        Arguments arguments;
        try {
            arguments = Arguments.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println(usage());
            return EXIT_BAD_INVOCATION;
        }
        if (arguments.help) {
            System.out.println(usage());
            return EXIT_OK;
        }

        NodeConfiguration configuration;
        try {
            configuration = NodeConfiguration.load(arguments.home, arguments.configFile);
        } catch (ConfigurationException e) {
            // The message names the file and the offending key; a stack trace through snakeyaml
            // would only bury it.
            LOG.error("Invalid configuration: {}", e.getMessage());
            return EXIT_BAD_INVOCATION;
        } catch (Exception e) {
            LOG.error("Cannot read {}", arguments.configFile, e);
            return EXIT_BAD_INVOCATION;
        }

        LOG.info("Starting cluster '{}' from {}", configuration.clusterName(), arguments.home);
        Supervisor supervisor = new Supervisor(configuration);
        try {
            supervisor.start();
        } catch (RuntimeException | Error e) {
            // start() has already stopped whatever it managed to bring up, in order.
            LOG.error("cassandra-opensearch failed to start: {}", e.getMessage(), e);
            return EXIT_FAILED;
        }
        try {
            return supervisor.awaitShutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            supervisor.stop();
            return EXIT_FAILED;
        }
    }

    private static String usage() {
        return """
                Usage: cassandra-opensearch-server [options]

                  --home <dir>      installation root (default: -Dcassandra-opensearch.home,
                                    then $CASSANDRA_OPENSEARCH_HOME, then the working directory)
                  --config <file>   supervisor configuration
                                    (default: <home>/conf/cassandra-opensearch.yaml)
                  --help            print this message

                Runs an Apache Cassandra node and an OpenSearch node in this JVM under one
                lifecycle. Stop it with SIGTERM, or retire it from both clusters with
                'bin/cassandra-opensearch decommission'.""";
    }

    /** Parsed command line, with the system-property and environment fallbacks already applied. */
    static final class Arguments {

        final Path home;
        final Path configFile;
        final boolean help;

        private Arguments(Path home, Path configFile, boolean help) {
            this.home = home;
            this.configFile = configFile;
            this.help = help;
        }

        static Arguments parse(String[] args) {
            Path home = null;
            Path configFile = null;
            boolean help = false;
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--home" -> home = Paths.get(value(args, i++, "--home"));
                    case "--config" -> configFile = Paths.get(value(args, i++, "--config"));
                    case "--help", "-h" -> help = true;
                    default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
                }
            }
            if (home == null) {
                home = Paths.get(defaultHome());
            }
            if (configFile == null) {
                String configured = System.getProperty(CONFIG_PROPERTY);
                configFile = configured != null
                        ? Paths.get(configured)
                        : home.resolve("conf").resolve("cassandra-opensearch.yaml");
            }
            return new Arguments(home.toAbsolutePath().normalize(),
                    configFile.toAbsolutePath().normalize(), help);
        }

        private static String defaultHome() {
            String property = System.getProperty(HOME_PROPERTY);
            if (property != null && !property.isBlank()) {
                return property;
            }
            String environment = System.getenv(HOME_ENVIRONMENT);
            if (environment != null && !environment.isBlank()) {
                return environment;
            }
            return System.getProperty("user.dir");
        }

        private static String value(String[] args, int index, String option) {
            if (index + 1 >= args.length) {
                throw new IllegalArgumentException(option + " needs a value");
            }
            return args[index + 1];
        }
    }
}
