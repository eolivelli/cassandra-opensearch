/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.spi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * {@link ServiceException#flatten} is the one thing standing between a failure inside an isolated
 * ClassLoader and a supervisor that can report it. It runs from a constructor, on whatever thread
 * failed, over a cause chain assembled by code this project does not own — so what it must do
 * above all is terminate.
 */
class ServiceExceptionTest {

    @Test
    void flattenRebuildsTheChainOutOfTypesThatExistEverywhere() {
        Exception root = new IllegalArgumentException("root cause");
        Exception middle = new IllegalStateException("middle", root);

        Throwable flattened = ServiceException.flatten(middle);

        assertThat(chainOf(flattened)).hasSize(2);
        assertThat(chainOf(flattened)).allSatisfy(link ->
                assertThat(link.getClass()).isSameAs(RuntimeException.class));
        assertThat(flattened).hasMessage("java.lang.IllegalStateException: middle");
        assertThat(flattened.getCause()).hasMessage("java.lang.IllegalArgumentException: root cause");
        assertThat(flattened.getStackTrace())
                .as("the stack trace is the only thing left that says where it happened")
                .isEqualTo(middle.getStackTrace());
    }

    @Test
    void flattenOfNullIsNull() {
        assertThat(ServiceException.flatten(null)).isNull();
    }

    /**
     * The defect. {@code b.initCause(a)} where {@code a}'s cause is {@code b} is legal —
     * {@link Throwable#initCause} rejects only self-causation — and recursing over it overflowed
     * the stack from inside {@code new ServiceException(...)}, which is to say the failure that
     * exists to make a cross-boundary failure reportable became unreportable itself.
     */
    @Test
    void aCyclicChainTerminatesAndSaysSo() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);

        Throwable flattened = ServiceException.flatten(a);

        List<Throwable> chain = chainOf(flattened);
        assertThat(chain).hasSize(3);
        assertThat(chain.get(0)).hasMessage("java.lang.RuntimeException: a");
        assertThat(chain.get(1)).hasMessage("java.lang.RuntimeException: b");
        assertThat(chain.get(2).getMessage())
                .as("recorded the way Throwable.printStackTrace records one")
                .startsWith("[CIRCULAR REFERENCE: java.lang.RuntimeException: a]");
    }

    @Test
    void theConstructorItselfSurvivesACyclicCause() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);

        ServiceException wrapped = new ServiceException("opensearch", "boom", a);

        assertThat(wrapped.getOriginalType()).isEqualTo("java.lang.RuntimeException");
        assertThat(wrapped.getServiceName()).isEqualTo("opensearch");
        assertThat(chainOf(wrapped.getCause())).hasSize(3);
    }

    @Test
    void aChainLongerThanTheCapIsCutRatherThanFollowed() {
        Throwable deep = chainOfLength(5_000);

        Throwable flattened = ServiceException.flatten(deep);

        List<Throwable> chain = chainOf(flattened);
        assertThat(chain).hasSize(ServiceException.MAX_FLATTENED + 1);
        assertThat(chain.get(chain.size() - 1).getMessage()).startsWith("[CHAIN TRUNCATED at ");
    }

    /**
     * The reviewer's reproduction, kept as the test: Cassandra sizes several of its pools with
     * {@code -Xss256k}, and a recursive flatten overflowed at roughly 5000 links there. The
     * traversal has to be iterative, not merely "deep enough for most chains".
     */
    @Test
    void aDeepChainDoesNotOverflowASmallStack() throws Exception {
        Throwable deep = chainOfLength(5_000);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Throwable> result = new AtomicReference<>();

        Thread small = new Thread(null,
                () -> {
                    try {
                        result.set(new ServiceException("cassandra", "deep", deep).getCause());
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                },
                "flatten-on-256k-stack",
                256 * 1024);
        small.start();
        small.join();

        assertThat(failure.get()).isNull();
        assertThat(chainOf(result.get())).hasSize(ServiceException.MAX_FLATTENED + 1);
    }

    /**
     * Suppressed exceptions were dropped entirely. A failure that also failed to close three
     * things carries the reason each one failed, and that record does not exist anywhere else.
     */
    @Test
    void suppressedExceptionsAreFlattenedAndReattached() {
        RuntimeException top = new RuntimeException("could not stop");
        RuntimeException whileClosing = new RuntimeException("socket would not close",
                new IllegalStateException("still bound"));
        top.addSuppressed(whileClosing);
        top.addSuppressed(new RuntimeException("file handle leaked"));

        Throwable flattened = ServiceException.flatten(top);

        assertThat(flattened.getSuppressed()).hasSize(2);
        assertThat(flattened.getSuppressed()[0])
                .hasMessage("java.lang.RuntimeException: socket would not close");
        assertThat(flattened.getSuppressed()[0].getCause())
                .as("a suppressed exception has a cause chain of its own, and it is flattened too")
                .hasMessage("java.lang.IllegalStateException: still bound");
        assertThat(flattened.getSuppressed()[1]).hasMessage("java.lang.RuntimeException: file handle leaked");
        assertThat(flattened.getSuppressed()).allSatisfy(s ->
                assertThat(s.getClass()).isSameAs(RuntimeException.class));
    }

    /** Suppression can be cyclic too, and it is reached by a different code path than causes. */
    @Test
    void aCycleThroughSuppressedExceptionsAlsoTerminates() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b");
        a.addSuppressed(b);
        b.addSuppressed(a);

        Throwable flattened = ServiceException.flatten(a);

        assertThat(flattened.getSuppressed()).hasSize(1);
        assertThat(flattened.getSuppressed()[0].getSuppressed()).hasSize(1);
        assertThat(flattened.getSuppressed()[0].getSuppressed()[0].getMessage())
                .startsWith("[CIRCULAR REFERENCE: java.lang.RuntimeException: a]");
    }

    @Test
    void aCauseWithNoMessageStillNamesItsType() {
        Throwable flattened = ServiceException.flatten(new NullPointerException());

        assertThat(flattened).hasMessage("java.lang.NullPointerException: null");
    }

    @Test
    void aNullCauseLeavesNoOriginalType() {
        ServiceException noCause = new ServiceException("cassandra", "nothing underneath");

        assertThat(noCause.getOriginalType()).isNull();
        assertThat(noCause.getCause()).isNull();
        assertThat(noCause.getServiceName()).isEqualTo("cassandra");
    }

    /** Building the wrapper must never be the thing that fails. */
    @Test
    void constructingFromAnyOfTheseNeverThrows() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);
        a.addSuppressed(b);

        assertThat(catchThrowable(() -> new ServiceException("probe", "m", a))).isNull();
        assertThat(catchThrowable(() -> new ServiceException("probe", "m", chainOfLength(20_000)))).isNull();
    }

    private static Throwable chainOfLength(int length) {
        Throwable current = new RuntimeException("link 0");
        for (int i = 1; i < length; i++) {
            current = new RuntimeException("link " + i, current);
        }
        return current;
    }

    /** {@code thrown} and everything under it, bounded so a cyclic chain cannot hang the test. */
    private static List<Throwable> chainOf(Throwable thrown) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable current = thrown;
             current != null && chain.size() < 10_000;
             current = current.getCause()) {
            chain.add(current);
        }
        return chain;
    }
}
