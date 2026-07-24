/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server.config;

/**
 * A configuration file is missing, malformed, or contains a setting the supervisor does not
 * understand.
 *
 * <p>Unchecked, because there is no useful recovery: the process refuses to start and the
 * operator fixes the file. The message is the entire user interface for that, so it always
 * names the file and the offending key.
 */
public class ConfigurationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
