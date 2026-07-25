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
     *
     * <p>It bounds <b>allocation</b>, which is a stronger claim than it sounds and than this code
     * used to honour: reaching the limit stops the traversal rather than merely stopping it from
     * reproducing anything, so a throwable with two hundred thousand suppressed exceptions
     * produces a hundred reproductions and a note, not two hundred thousand notes. On top of the
     * reproductions each truncation point costs one marker, and a marker is only ever emitted
     * where a reproduction was going to be — so the total is at most twice this, and in practice
     * a handful more than it.
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
     * <p>The traversal is iterative and tracks the throwables on the <b>path it is currently
     * following</b> by identity, so a cyclic chain terminates. {@code a.initCause(b)} with {@code
     * b}'s cause being {@code a} is legal — {@link Throwable#initCause} rejects only
     * self-causation — and a cycle is recorded the way {@link Throwable#printStackTrace} records
     * one, as {@code [CIRCULAR REFERENCE: ...]}. A chain longer than {@link #MAX_FLATTENED} is cut
     * with a marker naming where it was cut, for the same reason.
     *
     * <p>Per path, and not one set shared by the whole traversal, because those are different
     * questions and only the first one is "is this a cycle". A throwable reachable twice by two
     * different routes is an ordinary shape — one root cause, several failed {@code close()} calls
     * that each wrapped it — and a shared set reported the second route as {@code [CIRCULAR
     * REFERENCE]} and threw away everything under it, which is to say it lost the actual reason
     * the operation failed and blamed a cycle that was not there.
     */
    public static Throwable flatten(Throwable cause) {
        if (cause == null) {
            return null;
        }
        return flatten(cause, Collections.newSetFromMap(new IdentityHashMap<>()), new int[] {MAX_FLATTENED});
    }

    /**
     * @param path   the throwables between the root of this traversal and {@code cause}, by
     *               identity. A branch into a suppressed exception gets a copy: what its own
     *               ancestors are is what makes a repeat a cycle.
     * @param budget single-element cell holding the number of throwables still allowed to be
     *               allocated; shared by the whole traversal, so that the total is what is bounded
     */
    private static Throwable flatten(Throwable cause, Set<Throwable> path, int[] budget) {
        // Walk the cause chain first, iteratively: recursing here is what overflowed.
        List<Throwable> chain = new ArrayList<>();
        String terminator = null;
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (!path.add(current)) {
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
            // Counted too. A marker allocated for free is how the bound came to bound nothing:
            // every suppressed exception in a cyclic web reaches this line, and none of them paid.
            budget[0]--;
            flattened = marker(terminator);
        }
        for (int i = chain.size() - 1; i >= 0; i--) {
            Throwable original = chain.get(i);
            RuntimeException wrapper = new RuntimeException(describe(original), flattened);
            wrapper.setStackTrace(original.getStackTrace());
            attachSuppressed(wrapper, original, path, chain, i, budget);
            flattened = wrapper;
        }
        return flattened;
    }

    /**
     * Reproduces {@code original}'s suppressed exceptions onto {@code wrapper}, while the budget
     * lasts.
     *
     * <p>The check is <i>before</i> the recursion, and that is the whole fix: recursing with an
     * exhausted budget still allocated a marker for every remaining suppressed exception without
     * consuming anything, so the bound bounded nothing. A failure that suppressed two hundred
     * thousand others produced two hundred thousand markers against a documented limit of a
     * hundred — from a constructor, on a thread that had just failed.
     */
    private static void attachSuppressed(RuntimeException wrapper, Throwable original,
                                         Set<Throwable> path, List<Throwable> chain, int index,
                                         int[] budget) {
        Throwable[] suppressed = original.getSuppressed();
        for (int i = 0; i < suppressed.length; i++) {
            if (budget[0] <= 0) {
                budget[0]--;
                wrapper.addSuppressed(marker("[" + (suppressed.length - i) + " further suppressed"
                        + " exceptions dropped: the " + MAX_FLATTENED
                        + "-throwable budget was exhausted]"));
                return;
            }
            wrapper.addSuppressed(flatten(suppressed[i], branchPath(path, chain, index), budget));
        }
    }

    /**
     * The ancestors of {@code chain.get(index)}: whatever this traversal arrived with, plus the
     * part of the current chain at or above it. Not the part below it — a suppressed exception
     * that also appears deeper in its own carrier's cause chain is a repeat, not a cycle.
     */
    private static Set<Throwable> branchPath(Set<Throwable> path, List<Throwable> chain, int index) {
        Set<Throwable> branch = Collections.newSetFromMap(new IdentityHashMap<>());
        branch.addAll(path);
        branch.removeAll(chain.subList(index + 1, chain.size()));
        return branch;
    }

    private static RuntimeException marker(String message) {
        RuntimeException marker = new RuntimeException(message);
        marker.setStackTrace(new StackTraceElement[0]);
        return marker;
    }

    private static String describe(Throwable throwable) {
        return throwable.getClass().getName() + ": " + throwable.getMessage();
    }
}
