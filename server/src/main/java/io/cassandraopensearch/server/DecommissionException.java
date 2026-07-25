/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

/**
 * A coupled decommission could not be completed.
 *
 * <p>Checked, unlike the rest of the supervisor's failures, because this one <i>is</i>
 * recoverable and the caller has a decision to make: the node is still up and still serving when
 * this is thrown, and the operator's next move — wait, retry, or re-run with {@code --force} —
 * depends on which phase gave up. The message therefore always names the phase and says what
 * state the node was left in.
 */
public class DecommissionException extends Exception {

    private static final long serialVersionUID = 1L;

    public DecommissionException(String message) {
        super(message);
    }

    public DecommissionException(String message, Throwable cause) {
        super(message, cause);
    }
}
