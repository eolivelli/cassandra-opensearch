/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.spi;

/**
 * The {@code ring.state.changed} event, and the grammar of its message.
 *
 * <p>{@link ServiceContext#reportEvent} carries nothing but strings, because it has to cross the
 * ClassLoader boundary and the SPI may not grow a dependency. That makes the message format a
 * contract between two modules that share no code otherwise — the Cassandra runtime writes it,
 * the supervisor reads it and decides whether to react — and a contract expressed as "both sides
 * happen to format the string the same way" is one that rots at the first edit. Hence this type:
 * the grammar exists in exactly one place, and both sides compile against it.
 *
 * <p>{@link #leaving} rather than a mode name is what the supervisor keys on. The set of operation
 * modes that mean "this node is on its way out of its cluster" is the service's vocabulary, not
 * the supervisor's, so the service states the conclusion and the supervisor does not have to
 * carry a table of Cassandra's mode names. {@link #state} is carried alongside it because that is
 * what an operator reading the log needs to see.
 *
 * @param state   the service's own name for the state it has moved into, e.g. {@code "LEAVING"}
 * @param leaving true when {@code state} means this node is leaving its cluster for good
 * @param detail  human-readable context; never parsed
 */
public record RingStateEvent(String state, boolean leaving, String detail) {

    /** The {@code type} this event is reported under. */
    public static final String TYPE = "ring.state.changed";

    private static final String STATE_KEY = "state=";
    private static final String LEAVING_KEY = "leaving=";

    /** Renders this event as a {@link ServiceContext#reportEvent} message. */
    public String toMessage() {
        return STATE_KEY + state + ' ' + LEAVING_KEY + leaving + "; " + detail;
    }

    /**
     * Reads back a message produced by {@link #toMessage()}.
     *
     * <p>{@code leaving} must read exactly {@code true} or {@code false}. It is the field the
     * supervisor keys its whole reaction on, and {@link Boolean#parseBoolean} maps everything it
     * does not recognise — {@code yes}, {@code 1}, a typo — to {@code false}, which is the
     * "nothing to do here" verdict. Silently disabling the watchdog is the one outcome a
     * malformed message must not produce, so anything else rejects the message.
     *
     * @return the event, or null if the message is not in this grammar. Null is not a failure to
     *         report: an event of this type from a service that predates the grammar, or a
     *         message an operator has reworded, means only that the supervisor has nothing it can
     *         act on — and a parser that guessed would act on the wrong thing.
     */
    public static RingStateEvent parse(String message) {
        if (message == null) {
            return null;
        }
        int separator = message.indexOf(';');
        String head = separator < 0 ? message : message.substring(0, separator);
        String detail = separator < 0 ? "" : message.substring(separator + 1).trim();
        String state = null;
        String leaving = null;
        for (String token : head.trim().split("\\s+")) {
            if (token.startsWith(STATE_KEY)) {
                state = token.substring(STATE_KEY.length());
            } else if (token.startsWith(LEAVING_KEY)) {
                leaving = token.substring(LEAVING_KEY.length());
            }
        }
        if (state == null || state.isEmpty() || leaving == null) {
            return null;
        }
        if (!"true".equals(leaving) && !"false".equals(leaving)) {
            return null;
        }
        return new RingStateEvent(state, "true".equals(leaving), detail);
    }
}
