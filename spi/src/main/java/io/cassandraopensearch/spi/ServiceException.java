/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Thrown across the ClassLoader boundary when a lifecycle operation fails.
 *
 * <p>Exceptions raised inside an isolated service are usually of a type the supervisor cannot
 * load (a Cassandra or OpenSearch exception class), and letting one propagate would surface as
 * a confusing {@link NoClassDefFoundError}. Implementations therefore wrap failures in this
 * type, whose {@code cause} chain has already been flattened to {@link String} form by
 * {@link #flatten}.
 */
public class ServiceException extends Exception {

    private static final long serialVersionUID = 1L;

    private final String serviceName;
    private final String originalType;

    public ServiceException(String serviceName, String message) {
        this(serviceName, message, null);
    }

    public ServiceException(String serviceName, String message, Throwable cause) {
        super(message, flatten(cause));
        this.serviceName = serviceName;
        this.originalType = cause == null ? null : cause.getClass().getName();
    }

    /** The service that failed, e.g. {@code "opensearch"}. */
    public String getServiceName() {
        return serviceName;
    }

    /** Class name of the original, possibly unloadable, exception. May be null. */
    public String getOriginalType() {
        return originalType;
    }

    /**
     * Upper bound on the number of throwables one {@link #flatten} call reproduces, counting
     * causes and suppressed exceptions together.
     *
     * <p>A bound is required, not defensive: this runs from a constructor, on whatever thread
     * failed, and Cassandra sizes several of its pools with {@code -Xss256k}. A chain long enough
     * to exhaust that stack turns the one type that exists to make a cross-boundary failure
     * reportable into a {@link StackOverflowError} thrown from inside {@code new}.
     */
    static final int MAX_FLATTENED = 100;

    /**
     * Rebuilds a throwable's cause chain out of plain {@link RuntimeException}s carrying the
     * original type name and stack trace, so it can be thrown into a ClassLoader that cannot
     * load the original exception classes.
     *
     * <p>Suppressed exceptions are flattened and re-attached: a failure that closed several
     * resources on its way out carries the reason each one failed, and dropping them loses the
     * only record of it.
     *
     * <p>The traversal is iterative and tracks the throwables it has already seen by identity, so
     * a cyclic chain terminates. {@code a.initCause(b)} with {@code b}'s cause being {@code a} is
     * legal — {@link Throwable#initCause} rejects only self-causation — and a cycle is recorded
     * the way {@link Throwable#printStackTrace} records one, as
     * {@code [CIRCULAR REFERENCE: ...]}. A chain longer than {@link #MAX_FLATTENED} is cut with a
     * marker naming where it was cut, for the same reason.
     */
    public static Throwable flatten(Throwable cause) {
        if (cause == null) {
            return null;
        }
        return flatten(cause, Collections.newSetFromMap(new IdentityHashMap<>()), new int[] {MAX_FLATTENED});
    }

    /**
     * @param seen   every throwable already reproduced, by identity, shared across causes and
     *               suppressed exceptions alike — the two can point at each other
     * @param budget single-element cell holding the number of throwables still allowed; shared
     *               with the same reach as {@code seen}, so that the total is what is bounded
     */
    private static Throwable flatten(Throwable cause, Set<Throwable> seen, int[] budget) {
        // Walk the cause chain first, iteratively: recursing here is what overflowed.
        List<Throwable> chain = new ArrayList<>();
        String terminator = null;
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (!seen.add(current)) {
                terminator = "[CIRCULAR REFERENCE: " + describe(current) + "]";
                break;
            }
            if (budget[0] <= 0) {
                terminator = "[CHAIN TRUNCATED at " + MAX_FLATTENED + " throwables: "
                        + describe(current) + " ...]";
                break;
            }
            budget[0]--;
            chain.add(current);
        }

        // Then rebuild it from the innermost cause outwards, so every wrapper can be constructed
        // with its cause already in hand.
        Throwable flattened = null;
        if (terminator != null) {
            flattened = new RuntimeException(terminator);
            flattened.setStackTrace(new StackTraceElement[0]);
        }
        for (int i = chain.size() - 1; i >= 0; i--) {
            Throwable original = chain.get(i);
            RuntimeException wrapper = new RuntimeException(describe(original), flattened);
            wrapper.setStackTrace(original.getStackTrace());
            for (Throwable suppressed : original.getSuppressed()) {
                wrapper.addSuppressed(flatten(suppressed, seen, budget));
            }
            flattened = wrapper;
        }
        return flattened;
    }

    private static String describe(Throwable throwable) {
        return throwable.getClass().getName() + ": " + throwable.getMessage();
    }
}
