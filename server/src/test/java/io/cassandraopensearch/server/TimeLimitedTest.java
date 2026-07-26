/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.spi.ServiceException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The deadline every SPI lifecycle call is run behind.
 *
 * <p>The distinction this class exists to make is between a call that <i>failed</i> and a call
 * that was <i>abandoned</i>. The first is over; the second is still running inside a service, on a
 * thread nothing can stop, and the supervisor must not then stop that service or close its
 * ClassLoader — see {@code SupervisorLifecycleTest.anAbandonedStartupIsNeitherStoppedNorClosed}.
 */
class TimeLimitedTest {

    @Test
    void returnsWhatTheCallReturned() throws Exception {
        assertThat(TimeLimited.call("quick", Duration.ofSeconds(5), () -> "done")).isEqualTo("done");
    }

    @Test
    void runsTheCallOnADaemonThreadOfItsOwn() throws Exception {
        Thread caller = Thread.currentThread();
        Thread worker = TimeLimited.call("borrowed", Duration.ofSeconds(5), Thread::currentThread);

        assertThat(worker).isNotSameAs(caller);
        assertThat(worker.isDaemon())
                .as("the corpse of an overrunning call must not by itself keep the JVM alive")
                .isTrue();
    }

    /** A checked exception is the service's own diagnosis and has to arrive intact. */
    @Test
    void propagatesTheCallsOwnException() {
        ServiceException thrown = new ServiceException("cassandra", "no route to the seeds");

        assertThatThrownBy(() -> TimeLimited.run("failing", Duration.ofSeconds(5), () -> {
            throw thrown;
        })).isSameAs(thrown);
    }

    @Test
    void wrapsAnErrorRatherThanLosingIt() {
        assertThatThrownBy(() -> TimeLimited.run("linkage", Duration.ofSeconds(5), () -> {
            throw new NoClassDefFoundError("org/apache/cassandra/service/StorageService");
        }))
                .isInstanceOf(SupervisorException.class)
                .hasMessageContaining("linkage")
                .hasMessageContaining("StorageService");
    }

    /**
     * The overrun is reported as an {@link TimeLimited.AbandonedException}, not a plain
     * {@link SupervisorException}: the caller has to be able to tell "this is over" from "this is
     * still running and will keep running".
     */
    @Test
    void reportsAnOverrunAsAbandonedAndInterruptsTheWorker() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        CountDownLatch noticed = new CountDownLatch(1);

        assertThatThrownBy(() -> TimeLimited.run("slow startup", Duration.ofMillis(100), () -> {
            try {
                release.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                interrupted.set(true);
                noticed.countDown();
            }
        }))
                .isInstanceOf(TimeLimited.AbandonedException.class)
                .isInstanceOf(SupervisorException.class)
                .hasMessageContaining("slow startup")
                .hasMessageContaining("did not complete within 100ms")
                .hasMessageContaining("abandoned");

        assertThat(noticed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(interrupted).isTrue();
        release.countDown();
    }

    /**
     * The interrupt path used to be the second way this method returned while the call was still
     * running inside the service — and the only one that did not say so. It threw a plain {@code
     * SupervisorException}, which {@code Supervisor.stopService} funnels into its {@code catch
     * (Throwable)}: "did not stop cleanly", then {@code close()} on the loader that live call is
     * executing out of, then a final status recorded as if the service had settled, then exit 0.
     *
     * <p>The deadline is now the only thing that ends the wait. An interrupt is remembered, logged
     * and put back; the call is waited for as before.
     */
    @Test
    void anInterruptDoesNotEndTheWaitAndIsPutBackAfterwards() throws Exception {
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread caller = Thread.currentThread();

        Thread interrupter = new Thread(() -> {
            try {
                assertThat(inside.await(10, TimeUnit.SECONDS)).isTrue();
                caller.interrupt();
                // Long enough that a wait which obeyed the interrupt would already have returned.
                Thread.sleep(200);
                release.countDown();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }, "test-interrupter");
        interrupter.start();

        try {
            String result = TimeLimited.call("drain", Duration.ofSeconds(30), () -> {
                inside.countDown();
                release.await(10, TimeUnit.SECONDS);
                return "drained";
            });

            assertThat(result)
                    .as("the call finished; an interrupt of the waiter is not a failure of it")
                    .isEqualTo("drained");
            assertThat(Thread.currentThread().isInterrupted())
                    .as("not obeyed, but it still belongs to this thread")
                    .isTrue();
        } finally {
            Thread.interrupted();
            interrupter.join(TimeUnit.SECONDS.toMillis(30));
        }
    }

    /**
     * The cascade. {@code FutureTask.get} checks the flag on entry, so once one call had re-set it
     * every later call in the same shutdown failed instantly — the whole ordered walk collapsed in
     * milliseconds, every service "did not stop cleanly", every loader closed under a live call. The
     * production path that does exactly this is {@code CassandraOpenSearchServer.run}: {@code
     * Thread.currentThread().interrupt(); supervisor.stop();}.
     */
    @Test
    void anInterruptAlreadyPendingDoesNotCollapseTheCall() throws Exception {
        Thread.currentThread().interrupt();
        try {
            assertThat(TimeLimited.call("stop cassandra", Duration.ofSeconds(30), () -> {
                Thread.sleep(50);
                return "stopped";
            })).isEqualTo("stopped");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * And when the deadline does expire on an interrupted waiter, the answer is the same as for
     * any other overrun: abandoned, because the call is still running.
     */
    @Test
    void anOverrunIsStillAbandonedWhenTheWaiterWasAlsoInterrupted() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> TimeLimited.run("slow stop", Duration.ofMillis(200),
                    () -> Thread.sleep(TimeUnit.MINUTES.toMillis(1))))
                    .isInstanceOf(TimeLimited.AbandonedException.class)
                    .hasMessageContaining("did not complete within 200ms");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * {@code await} is the same policy for the latches that say "the thread driving these services
     * has let go of them". A {@code false} there is answered by abandoning both services, so an
     * interrupt must not be able to produce one — the shutdown reported "the coordinator burned its
     * whole 1m budget" 4 ms after being asked.
     */
    @Test
    void awaitKeepsWaitingWhenInterruptedAndReportsTheLatchNotTheInterrupt() throws Exception {
        CountDownLatch finished = new CountDownLatch(1);
        Thread caller = Thread.currentThread();
        Thread interrupter = new Thread(() -> {
            try {
                Thread.sleep(100);
                caller.interrupt();
                Thread.sleep(200);
                finished.countDown();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }, "test-interrupter");
        interrupter.start();

        try {
            long start = System.nanoTime();
            boolean opened = TimeLimited.await(finished, Duration.ofSeconds(30));
            Duration waited = Duration.ofNanos(System.nanoTime() - start);

            assertThat(opened)
                    .as("the latch did open, well inside the budget; the interrupt was not an answer")
                    .isTrue();
            assertThat(waited).isGreaterThan(Duration.ofMillis(200));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
            interrupter.join(TimeUnit.SECONDS.toMillis(30));
        }
    }

    /** And the budget still ends it, with the latch's own verdict. */
    @Test
    void awaitReportsFalseWhenTheBudgetGenuinelyExpires() {
        assertThat(TimeLimited.await(new CountDownLatch(1), Duration.ofMillis(100))).isFalse();
        assertThat(TimeLimited.await(new CountDownLatch(0), Duration.ofMillis(100))).isTrue();
    }

    @Test
    void formatsDurationsTheWayTheConfigurationFileWritesThem() {
        assertThat(TimeLimited.format(Duration.ZERO)).isEqualTo("0s");
        assertThat(TimeLimited.format(Duration.ofHours(2))).isEqualTo("2h");
        assertThat(TimeLimited.format(Duration.ofMinutes(5))).isEqualTo("5m");
        assertThat(TimeLimited.format(Duration.ofMinutes(90))).isEqualTo("90m");
        assertThat(TimeLimited.format(Duration.ofSeconds(30))).isEqualTo("30s");
        assertThat(TimeLimited.format(Duration.ofMillis(500))).isEqualTo("500ms");
    }

    /**
     * Every branch above divides {@code toMillis()}, which is zero for a sub-millisecond duration
     * — and zero is divisible by everything, so the first test won and 500µs was rendered "0h".
     * Timeouts are reported to operators in exactly these strings.
     */
    @Test
    void doesNotRenderASubMillisecondDurationAsAnHour() {
        assertThat(TimeLimited.format(Duration.ofNanos(500_000))).isEqualTo("500000ns");
        assertThat(TimeLimited.format(Duration.ofNanos(1))).isEqualTo("1ns");
        assertThat(TimeLimited.format(Duration.ofMillis(-30_000))).isEqualTo("-30s");
    }
}
