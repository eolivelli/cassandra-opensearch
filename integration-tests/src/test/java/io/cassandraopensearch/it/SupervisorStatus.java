/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What {@code bin/cassandra-opensearch status} printed.
 *
 * <p>Read as text, from the outside, because that is the interface: the CLI discovers the
 * supervisor's attributes reflectively through {@code MBeanInfo} and prints whatever it finds, so
 * the printed page is the contract an operator actually has. Connecting to the MBean from the
 * test instead would assert a contract nobody uses.
 *
 * <p>Both the supervisor's own attributes and each service's {@code details()} are rendered as
 * {@code key : value} lines, at different indentation; the key is unique across the page, so one
 * lookup serves both.
 */
record SupervisorStatus(String text) {

    static SupervisorStatus of(Commands.Result result) {
        if (!result.succeeded()) {
            throw new AssertionError("status failed:\n" + result.describe());
        }
        return new SupervisorStatus(result.output());
    }

    String value(String key) {
        Matcher matcher = Pattern.compile("(?m)^\\s*" + Pattern.quote(key) + "\\s+:\\s*(.*?)\\s*$")
                .matcher(text);
        if (!matcher.find()) {
            throw new AssertionError("no '" + key + "' in the status output:\n" + text.indent(2));
        }
        return matcher.group(1);
    }

    /** The port out of an {@code address:port} or {@code host/1.2.3.4:port} detail value. */
    int port(String key) {
        String value = value(key);
        return Integer.parseInt(value.substring(value.lastIndexOf(':') + 1).replaceAll("[^0-9].*$", ""));
    }
}
