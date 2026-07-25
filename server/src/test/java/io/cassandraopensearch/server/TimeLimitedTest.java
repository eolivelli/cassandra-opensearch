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
