/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

/**
 * A lifecycle step the supervisor drives failed or timed out.
 *
 * <p>Unchecked, for the same reason {@code ConfigurationException} is: there is no recovery from
 * "Cassandra did not come up". The supervisor unwinds whatever it started and the process exits
 * non-zero, so the message is the whole diagnosis the operator gets and always names the service
 * and the step.
 */
public class SupervisorException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SupervisorException(String message) {
        super(message);
    }

    public SupervisorException(String message, Throwable cause) {
        super(message, cause);
    }
}
