/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.bootstrap;

import io.cassandraopensearch.spi.EmbeddedService;
import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Proves the isolation properties the whole design rests on, using a service implementation
 * compiled at test time into a jar that is deliberately <b>not</b> on the test classpath.
 *
 * <p>Testing this with a class the test JVM can already see would prove nothing: every
 * assertion would pass against a supervisor-loaded copy while the isolation quietly did
 * nothing.
 */
class IsolatedClassLoaderTest {

    private static final String IMPL = "probe.ProbeService";

    private static Path jar;

    @BeforeAll
    static void buildProbeJar(@TempDir Path tempDir) throws Exception {
        jar = ProbeServiceCompiler.compileToJar(tempDir);
    }

    private static List<URL> classpath() throws Exception {
        return List.of(jar.toUri().toURL());
    }

    @Test
    void loadsTheImplementationInTheIsolatedLoaderNotTheApplicationLoader() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            Class<?> loaded = Class.forName(IMPL, false, service.classLoader());
            assertThat(loaded.getClassLoader())
                    .as("implementation must belong to the isolated loader")
                    .isSameAs(service.classLoader());
        }
    }

    @Test
    void applicationLoaderCannotSeeTheIsolatedImplementation() {
        assertThatThrownBy(() -> Class.forName(IMPL, false, getClass().getClassLoader()))
                .as("if the supervisor can load it, the jar leaked onto lib/boot")
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void spiTypesResolveToTheSameClassObjectOnBothSides() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            Class<?> insideView = Class.forName(EmbeddedService.class.getName(), false, service.classLoader());

            // This is the single most important property here. If the isolated loader resolved
            // its own copy of the SPI, every cross-boundary call would fail with
            // ClassCastException, and the supervisor could not hold the service at all.
            assertThat(insideView).isSameAs(EmbeddedService.class);
        }
    }

    @Test
    void jdkTypesStillResolveThroughThePlatformLoader() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            assertThat(Class.forName("java.lang.String", false, service.classLoader()))
                    .isSameAs(String.class);
            assertThat(Class.forName("java.sql.Date", false, service.classLoader()))
                    .isSameAs(java.sql.Date.class);
        }
    }

    @Test
    void twoServicesGetIndependentCopiesOfTheSameClass() throws Exception {
        try (IsolatedService first = IsolatedService.create("probe-a", IMPL, classpath());
             IsolatedService second = IsolatedService.create("probe-b", IMPL, classpath())) {

            Class<?> a = Class.forName(IMPL, false, first.classLoader());
            Class<?> b = Class.forName(IMPL, false, second.classLoader());

            // The point of the exercise: same name, same jar, two unrelated Class objects with
            // two unrelated sets of statics. This is what lets Cassandra and OpenSearch each
            // believe they own the JVM.
            assertThat(a).isNotSameAs(b);
            assertThat(a.getName()).isEqualTo(b.getName());
        }
    }

    @Test
    void lifecycleCallsCrossTheBoundaryAndSeeTheIsolatedContextClassLoader() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            assertThat(service.status()).isEqualTo(ServiceStatus.NEW);

            service.start(new RecordingServiceContext());
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);

            // The probe records the TCCL it observed inside start(). Cassandra and OpenSearch
            // both reach for it during startup, so getting this wrong breaks them in ways that
            // are painful to diagnose.
            assertThat(service.details())
                    .containsEntry("tccl", "isolated-probe")
                    .containsEntry("name", "probe");

            service.stop();
            assertThat(service.status()).isEqualTo(ServiceStatus.STOPPED);
        }
    }

    @Test
    void exceptionsFromInsideArriveAsLoadableTypes() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            context.settings().put("probe.failOnStart", "true");

            // The probe throws an exception class that exists only inside its own jar. The
            // supervisor must still get something it can load, print and log.
            Throwable thrown = catchThrowable(() -> service.start(context));
            assertThat(thrown)
                    .isInstanceOf(ServiceException.class)
                    .hasMessageContaining("probe was asked to fail");

            // The assertions above pass whether or not the cause chain was translated, which is
            // the whole failure mode: the exception at the top is a ServiceException because the
            // probe made it one, and everything interesting is underneath it. That the *cause* is
            // loadable here is the property being tested.
            assertThat(unloadableTypesIn(thrown))
                    .as("every link of the chain the supervisor receives must be a type it can load")
                    .isEmpty();
            assertThat(causeChainOf(thrown))
                    .as("the isolated failure must still be legible, not erased")
                    .anySatisfy(link -> assertThat(link.getMessage())
                            .contains("probe.ProbeService$ProbeFailure")
                            .contains("thrown from inside the isolated loader"));
        }
    }

    /**
     * The defect this pins: nothing in {@code bootstrap} used to translate at all, so an
     * exception class that exists only in the service's own jar arrived at the supervisor exactly
     * as thrown. Two things follow, and the second is the expensive one — the supervisor cannot
     * print it without a {@link NoClassDefFoundError}, and anything that retains it retains the
     * isolated ClassLoader and every class behind it.
     */
    @Test
    void aRawIsolatedExceptionIsWrappedBeforeItReachesTheSupervisor() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            context.settings().put("probe.failOnStart", "raw");

            Throwable thrown = catchThrowable(() -> service.start(context));

            assertThat(thrown).isInstanceOf(ServiceException.class);
            assertThat(((ServiceException) thrown).getOriginalType())
                    .as("the original type has to survive as data, since it cannot survive as a type")
                    .isEqualTo("probe.ProbeService$ProbeFailure");
            assertThat(((ServiceException) thrown).getServiceName()).isEqualTo("probe");
            assertThat(thrown).hasMessageContaining("raw failure from inside the isolated loader");
            assertThat(unloadableTypesIn(thrown)).isEmpty();

            // ...and the type it replaced is genuinely one the supervisor could not have held.
            assertThatThrownBy(() ->
                    Class.forName("probe.ProbeService$ProbeFailure", false, getClass().getClassLoader()))
                    .isInstanceOf(ClassNotFoundException.class);
        }
    }

    /** Same for an {@link Error}: {@code catch (Exception)} would have let this one straight out. */
    @Test
    void aRawIsolatedErrorIsWrappedToo() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            context.settings().put("probe.failOnStart", "raw-error");

            Throwable thrown = catchThrowable(() -> service.start(context));

            assertThat(thrown).isInstanceOf(ServiceException.class);
            assertThat(((ServiceException) thrown).getOriginalType())
                    .isEqualTo("probe.ProbeService$ProbeError");
            assertThat(unloadableTypesIn(thrown)).isEmpty();
        }
    }

    /**
     * The other half of the rule. Translating unconditionally would bury every ordinary failure
     * under a wrapper and break callers that legitimately match on the type — the runtimes' own
     * tests assert {@code ServiceException} with a particular message, and the CLI matches on
     * {@code IllegalArgumentException}.
     */
    @Test
    void anExceptionTheSupervisorCanLoadArrivesExactlyAsThrown() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            context.settings().put("probe.failOnStart", "loadable");

            assertThatThrownBy(() -> service.start(context))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("probe refuses to start")
                    .hasNoCause();
        }
    }

    /** A loadable exception is not enough: the cause is what carries the loader across. */
    @Test
    void aLoadableExceptionWithAnIsolatedCauseIsStillWrapped() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            context.settings().put("probe.failOnStart", "loadable-with-isolated-cause");

            Throwable thrown = catchThrowable(() -> service.start(context));

            assertThat(thrown).isInstanceOf(ServiceException.class);
            assertThat(unloadableTypesIn(thrown)).isEmpty();
            assertThat(causeChainOf(thrown))
                    .anySatisfy(link -> assertThat(link.getMessage())
                            .contains("the real reason, unloadable outside"));
        }
    }

    /**
     * A cyclic cause chain is legal — {@code initCause} rejects only self-causation — and used to
     * take the translation into infinite recursion from inside a constructor.
     */
    @Test
    void aCyclicIsolatedChainCrossesTheBoundaryWithoutOverflowing() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            context.settings().put("probe.failOnStart", "cyclic");

            Throwable thrown = catchThrowable(() -> service.start(context));

            assertThat(thrown).isInstanceOf(ServiceException.class);
            assertThat(unloadableTypesIn(thrown)).isEmpty();
            assertThat(causeChainOf(thrown))
                    .hasSizeLessThan(200)
                    .last()
                    .satisfies(last -> assertThat(last.getMessage()).startsWith("[CIRCULAR REFERENCE:"));
        }
    }

    /**
     * {@code status()} caught {@link Exception}, so an {@link Error} from the delegate escaped.
     * {@code Supervisor.stopService} calls {@code status()} after {@code close()}, outside any
     * try, and an escape there aborts the shutdown before it records the final state, removes the
     * shutdown hook or counts the process down — the JVM then hangs on exit. This is that
     * sequence.
     */
    @Test
    void statusAndDetailsSurviveTheirOwnLoaderHavingBeenClosed() throws Exception {
        IsolatedService service = IsolatedService.create("probe", IMPL, classpath());
        RecordingServiceContext context = new RecordingServiceContext();
        service.start(context);
        assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);

        // From here on the probe reaches for a class it has never loaded, which is what a real
        // delegate does on any call into code it has not touched yet.
        context.settings().put("probe.statusMode", "lazy");
        context.settings().put("probe.detailsMode", "lazy");
        service.close();

        assertThatCode(service::status).doesNotThrowAnyException();
        assertThatCode(service::details).doesNotThrowAnyException();
        assertThat(service.status())
                .as("the last status actually observed, not a verdict invented on the way out")
                .isEqualTo(ServiceStatus.RUNNING);
        assertThat(service.details()).containsKey("error");
    }

    /**
     * "Could not determine the status" is not "the service has failed", and the difference is a
     * process: {@code Supervisor.checkService} turns {@code FAILED} into a shutdown of the whole
     * JVM, taking down the other server with it.
     */
    @Test
    void statusReportsTheLastKnownStateRatherThanFailedWhenItCannotBeRead() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            service.start(context);
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);

            for (String mode : List.of("isolated", "error")) {
                context.settings().put("probe.statusMode", mode);
                assertThat(service.status())
                        .as("status mode %s must not be read as a failure of the service", mode)
                        .isEqualTo(ServiceStatus.RUNNING)
                        .isNotEqualTo(ServiceStatus.FAILED);
                assertThat(service.details()).containsEntry("name", "probe");
            }

            context.settings().remove("probe.statusMode");
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
        }
    }

    /**
     * Catching an {@link InterruptedException} clears the interrupt that produced it, and
     * {@code status()} may run on any thread — including one the supervisor has just asked to
     * stop. Swallowing the flag there leaves that thread waiting for a signal already sent.
     */
    @Test
    void statusPutsBackAnInterruptItConsumed() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            service.start(context);
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
            assertThat(Thread.currentThread().isInterrupted()).isFalse();

            context.settings().put("probe.statusMode", "interrupt");
            try {
                assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
                assertThat(Thread.currentThread().isInterrupted())
                        .as("the interrupt was consumed by the catch and has to be put back")
                        .isTrue();
            } finally {
                // Clear it before close(), and before the next test inherits this thread.
                Thread.interrupted();
                context.settings().remove("probe.statusMode");
            }
        }
    }

    /**
     * The same, for the shape the interrupt actually arrives in.
     *
     * <p>A bare {@code InterruptedException} out of a delegate is the easy case and the one that
     * was handled. What both real loaders produce is a wrapped one — Netty's {@code
     * PlatformDependent.throwException} is inside each of them, and both runtimes wrap what their
     * executors throw — and matching only on the outermost type left the flag clear in exactly the
     * case that matters.
     */
    @Test
    void statusPutsBackAnInterruptCarriedInsideAWrappedFailure() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            service.start(context);
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
            assertThat(Thread.interrupted()).isFalse();

            context.settings().put("probe.statusMode", "wrapped-interrupt");
            try {
                assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
                assertThat(Thread.currentThread().isInterrupted())
                        .as("the interrupt was consumed by a catch two levels up from the"
                                + " InterruptedException, and still has to be put back")
                        .isTrue();
            } finally {
                Thread.interrupted();
                context.settings().remove("probe.statusMode");
            }
        }
    }

    /**
     * The third signal. {@code status()} answering with the last value it managed to read is the
     * right trade — {@code FAILED} would take the JVM down over a status that could not be read —
     * but on its own it makes a dead service indistinguishable from a healthy one: it reports
     * {@code RUNNING} to JMX and to the CLI forever. "Cannot be asked" now says so, in {@code
     * details()}, where nothing automated acts on it.
     */
    @Test
    void detailsSayWhenTheStatusIsStaleRatherThanFreshlyRead() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            service.start(context);
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
            assertThat(service.details()).doesNotContainKey("status_stale");

            context.settings().put("probe.statusMode", "error");
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);

            assertThat(service.details())
                    .as("the reported status is two polls old and nothing else says so")
                    .containsEntry("status_stale", "true")
                    .containsEntry("status_read_failures", "2");

            context.settings().remove("probe.statusMode");
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
            assertThat(service.details())
                    .as("and a service that answers again is not stale")
                    .doesNotContainKey("status_stale");
        }
    }

    /**
     * A {@code null} from the delegate is the one way of not answering that did not count as one.
     *
     * <p>{@code consecutiveStatusFailures} was reset to zero on every call that did not throw, and
     * the stale value was returned regardless — so a delegate returning null forever reported
     * {@code RUNNING} to JMX and to {@code bin/cassandra-opensearch status} with no {@code
     * status_stale} key anywhere, which is the single case that signal was added to make
     * impossible. The project treats a null status as a real thing elsewhere: {@code
     * Supervisor.recordFinalStatus} guards against one, and {@code RecordingService} has a switch
     * for producing it.
     */
    @Test
    void aNullStatusCountsAsAStatusThatCouldNotBeRead() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            RecordingServiceContext context = new RecordingServiceContext();
            service.start(context);
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
            assertThat(service.details()).doesNotContainKey("status_stale");

            context.settings().put("probe.statusMode", "null");
            assertThat(service.status())
                    .as("never null to the supervisor: it feeds a ConcurrentHashMap and a health"
                            + " check that would rather have the last value actually observed")
                    .isEqualTo(ServiceStatus.RUNNING);
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);

            assertThat(service.details())
                    .as("and the operator has to be able to tell this from a healthy node")
                    .containsEntry("status_stale", "true")
                    .containsEntry("status_read_failures", "2");

            context.settings().remove("probe.statusMode");
            assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
            assertThat(service.details()).doesNotContainKey("status_stale");
        }
    }

    /**
     * {@code details()} used to hand the supervisor the delegate's own Map instance. Its class
     * belongs to the isolated loader, so every consumer that keeps a status response keeps the
     * loader.
     */
    @Test
    void detailsCrossTheBoundaryAsASupervisorOwnedMap() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            service.start(new RecordingServiceContext());

            Map<String, String> details = service.details();

            assertThat(details.getClass())
                    .as("a Map type from the probe jar would pin the loader for as long as the map lives")
                    .isSameAs(LinkedHashMap.class);
            assertThat(details.getClass().getClassLoader()).isNotSameAs(service.classLoader());
            assertThat(details).containsEntry("name", "probe");
        }
    }

    /**
     * {@code close()} used to run {@code loader.close()} in a {@code finally}, so a loader that
     * would not close replaced — and erased — the reason the service would not stop, which is
     * usually the cause of the other.
     */
    @Test
    void closeReportsTheStopFailureAndStillReleasesTheLoader() throws Exception {
        IsolatedService service = IsolatedService.create("probe", IMPL, classpath());
        RecordingServiceContext context = new RecordingServiceContext();
        context.settings().put("probe.failOnStop", "held open by a stuck thread");
        service.start(context);

        Throwable thrown = catchThrowable(service::close);

        assertThat(thrown).isInstanceOf(ServiceException.class);
        assertThat(thrown).hasMessageContaining("probe refused to stop");
        assertThat(unloadableTypesIn(thrown)).isEmpty();

        // The loader still had to be released: leaving a whole server's classes behind because
        // stop() threw is the worse of the two outcomes.
        assertThatThrownBy(() -> Class.forName("probe.LazyProbe", false, service.classLoader()))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void closingReleasesTheLoader() throws Exception {
        IsolatedService service = IsolatedService.create("probe", IMPL, classpath());
        service.start(new RecordingServiceContext());
        service.close();

        // probe.LazyProbe exists in the jar but was never loaded, so this reaches the jar file
        // rather than the loaded-class cache. A closed URLClassLoader has released its jar
        // handles and can no longer satisfy it.
        assertThatThrownBy(() -> Class.forName("probe.LazyProbe", false, service.classLoader()))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void closeStopsAServiceThatHasAlreadyReachedATerminalState() throws Exception {
        IsolatedService service = IsolatedService.create("probe", IMPL, classpath());
        service.start(new RecordingServiceContext());
        service.decommission(new ImmediateDecommissionContext());
        assertThat(service.status()).isEqualTo(ServiceStatus.DECOMMISSIONED);

        service.close();

        // DECOMMISSIONED is terminal, but a decommissioned service has only left the cluster —
        // its node, threads and sockets are still open. Treating "terminal" as "nothing left to
        // release" leaked all of it.
        assertThat(service.details()).containsEntry("stopped", "true");
    }

    @Test
    void applicationClasspathIsInvisibleToTheIsolatedLoader() throws Exception {
        try (IsolatedService service = IsolatedService.create("probe", IMPL, classpath())) {
            // ClasspathResolver sits on the application classpath and is not in the probe jar.
            // Because the isolated loader's parent is the *platform* loader, the application
            // classpath is simply not reachable — this is what stops Cassandra's Guava from
            // being satisfied by the supervisor's, or vice versa.
            assertThatThrownBy(() ->
                    Class.forName(ClasspathResolver.class.getName(), false, service.classLoader()))
                    .isInstanceOf(ClassNotFoundException.class);

            // ...while the same name resolves perfectly well from the supervisor's side.
            assertThat(Class.forName(ClasspathResolver.class.getName(), false, getClass().getClassLoader()))
                    .isSameAs(ClasspathResolver.class);
        }
    }

    // --- helpers ---------------------------------------------------------------------------

    /** The cause chain below {@code thrown}, bounded so a cyclic chain cannot hang the test. */
    private static List<Throwable> causeChainOf(Throwable thrown) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable current = thrown.getCause();
             current != null && chain.size() < 500;
             current = current.getCause()) {
            chain.add(current);
        }
        return chain;
    }

    /**
     * The names of every type in {@code thrown}'s causes and suppressed exceptions that the
     * supervisor's own ClassLoader would not resolve to that same class — which is the operative
     * definition of "this exception cannot cross the boundary". Same name is not enough: two
     * loaders can each hold a class of one name, and only one of them is the one in hand.
     */
    private List<String> unloadableTypesIn(Throwable thrown) {
        List<String> unloadable = new ArrayList<>();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Throwable> pending = new ArrayDeque<>();
        pending.add(thrown);
        while (!pending.isEmpty() && seen.size() < 500) {
            Throwable current = pending.poll();
            if (!seen.add(current)) {
                continue;
            }
            Class<?> type = current.getClass();
            try {
                if (Class.forName(type.getName(), false, getClass().getClassLoader()) != type) {
                    unloadable.add(type.getName());
                }
            } catch (ClassNotFoundException | LinkageError e) {
                unloadable.add(type.getName());
            }
            if (current.getCause() != null) {
                pending.add(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return unloadable;
    }
}
