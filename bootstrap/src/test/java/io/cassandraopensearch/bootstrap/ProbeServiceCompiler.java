/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.bootstrap;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

/**
 * Compiles a throwaway {@code EmbeddedService} implementation into a jar at test time.
 *
 * <p>The jar exists so the isolation tests have a class that is genuinely absent from the test
 * JVM's classpath. Checking in a prebuilt jar would work too, but a compiled-on-demand one stays
 * in step with the SPI: if a method is added to {@code EmbeddedService}, this fails to compile
 * rather than failing mysteriously at load time.
 */
final class ProbeServiceCompiler {

    private ProbeServiceCompiler() {
    }

    /** The probe reports the context ClassLoader it saw, which is how the TCCL test observes it. */
    private static final String PROBE_SOURCE = """
            package probe;

            import io.cassandraopensearch.spi.DecommissionContext;
            import io.cassandraopensearch.spi.EmbeddedService;
            import io.cassandraopensearch.spi.ServiceContext;
            import io.cassandraopensearch.spi.ServiceException;
            import io.cassandraopensearch.spi.ServiceStatus;
            import java.util.LinkedHashMap;
            import java.util.Map;

            public class ProbeService implements EmbeddedService {

                /** Exists only inside the probe jar, so the supervisor cannot load it. */
                public static class ProbeFailure extends RuntimeException {
                    public ProbeFailure(String message) { super(message); }
                    public ProbeFailure(String message, Throwable cause) { super(message, cause); }
                }

                /** Likewise unloadable by the supervisor, and an Error rather than an Exception. */
                public static class ProbeError extends Error {
                    public ProbeError(String message) { super(message); }
                }

                /**
                 * Resolved during class initialisation, so details() keeps working after this
                 * loader has been closed. Resolution failure after close is a real behaviour and
                 * is exercised deliberately, through probe.detailsMode=lazy, rather than by
                 * accident here.
                 */
                private static final Class<?> DETAILS_TYPE = ProbeDetails.class;

                private volatile ServiceStatus status = ServiceStatus.NEW;
                private volatile String observedTccl = "<none>";
                private volatile boolean stopped = false;
                private volatile ServiceContext context;

                @Override
                public String name() { return "probe"; }

                @Override
                public void start(ServiceContext context) throws Exception {
                    this.context = context;
                    status = ServiceStatus.STARTING;
                    observedTccl = String.valueOf(Thread.currentThread().getContextClassLoader().getName());
                    String failure = context.settings().get("probe.failOnStart");
                    if (failure != null && !failure.isEmpty()) {
                        status = ServiceStatus.FAILED;
                        Throwable thrown = startFailure(failure);
                        if (thrown instanceof Error) {
                            throw (Error) thrown;
                        }
                        throw (Exception) thrown;
                    }
                    status = ServiceStatus.RUNNING;
                }

                /**
                 * The shapes a real runtime fails in, all of which have to reach the supervisor
                 * as something it can load, print and hold without pinning this loader.
                 */
                private Throwable startFailure(String mode) {
                    switch (mode) {
                        case "true":
                            // Already the reportable form.
                            return new ServiceException("probe", "probe was asked to fail",
                                    new ProbeFailure("thrown from inside the isolated loader"));
                        case "raw":
                            // The bad case: a type that exists only in this jar, thrown bare.
                            return new ProbeFailure("raw failure from inside the isolated loader");
                        case "raw-error":
                            return new ProbeError("raw error from inside the isolated loader");
                        case "loadable":
                            // Nothing to translate; must arrive exactly as thrown.
                            return new IllegalStateException("probe refuses to start");
                        case "loadable-with-isolated-cause":
                            // Looks harmless from the outside; the cause is what pins the loader.
                            return new IllegalStateException("probe refuses to start",
                                    new ProbeFailure("the real reason, unloadable outside"));
                        case "cyclic":
                            // Legal: initCause only rejects self-causation.
                            ProbeFailure inner = new ProbeFailure("inner");
                            ProbeFailure outer = new ProbeFailure("outer", inner);
                            inner.initCause(outer);
                            return outer;
                        default:
                            return new ProbeFailure("unknown failure mode " + mode);
                    }
                }

                @Override
                public ServiceStatus status() {
                    failIfAsked("probe.statusMode");
                    return status;
                }

                @Override
                public Map<String, String> details() {
                    failIfAsked("probe.detailsMode");
                    // Deliberately a Map type from this jar: handing it out unchanged hands the
                    // supervisor an object whose class pins this loader.
                    Map<String, String> details = new ProbeDetails();
                    details.put("name", name());
                    details.put("tccl", observedTccl);
                    details.put("loader", String.valueOf(getClass().getClassLoader().getName()));
                    details.put("stopped", String.valueOf(stopped));
                    return details;
                }

                /**
                 * status() and details() are the two calls the supervisor makes after close(),
                 * so they are the two that have to survive their own loader being gone.
                 */
                private void failIfAsked(String key) {
                    ServiceContext seen = context;
                    String mode = seen == null ? null : seen.settings().get(key);
                    if (mode == null || mode.isEmpty()) {
                        return;
                    }
                    if ("isolated".equals(mode)) {
                        throw new ProbeFailure("asked to fail in " + key);
                    }
                    if ("error".equals(mode)) {
                        throw new ProbeError("asked to fail in " + key);
                    }
                    if ("interrupt".equals(mode)) {
                        // status() cannot declare a checked exception, so the only way one gets
                        // out is unchecked-ly — which is exactly what Netty's
                        // PlatformDependent.throwException does, and Netty is inside both of the
                        // real loaders.
                        ProbeService.<RuntimeException>sneakyThrow(
                                new InterruptedException("interrupted inside " + key));
                    }
                    if ("lazy".equals(mode)) {
                        // Exactly what the JVM does when this loader has been closed and the
                        // class was never loaded: a resolution failure, and an Error, not an
                        // Exception.
                        try {
                            Class.forName("probe.LazyProbe", true, ProbeService.class.getClassLoader());
                        } catch (ClassNotFoundException e) {
                            throw new NoClassDefFoundError("probe/LazyProbe");
                        }
                        return;
                    }
                    throw new IllegalStateException("unknown mode " + mode + " for " + key);
                }

                @SuppressWarnings("unchecked")
                private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
                    throw (T) t;
                }

                @Override
                public void prepareDecommission(DecommissionContext context) {
                    status = ServiceStatus.DECOMMISSIONING;
                }

                @Override
                public boolean awaitDecommissionReady(DecommissionContext context) { return true; }

                @Override
                public void abortDecommission(DecommissionContext context) {
                    if (status == ServiceStatus.DECOMMISSIONING) {
                        status = ServiceStatus.RUNNING;
                    }
                }

                @Override
                public void decommission(DecommissionContext context) {
                    status = ServiceStatus.DECOMMISSIONED;
                }

                @Override
                public void stop() {
                    stopped = true;
                    ServiceContext seen = context;
                    String failure = seen == null ? null : seen.settings().get("probe.failOnStop");
                    if (failure != null && !failure.isEmpty()) {
                        throw new ProbeFailure("probe refused to stop: " + failure);
                    }
                    // Mirrors the real services: a decommissioned node stays DECOMMISSIONED
                    // after stop(), because that is the state the operator asked for.
                    if (status != ServiceStatus.DECOMMISSIONED) {
                        status = ServiceStatus.STOPPED;
                    }
                }
            }
            """;

    /** Never loaded by the tests; used to prove a closed loader has released its jar. */
    private static final String LAZY_SOURCE = """
            package probe;

            public class LazyProbe {
                public static final String MARKER = "never-loaded-until-asked";
            }
            """;

    /**
     * The Map implementation {@code details()} returns. It is a type from this jar, so an
     * implementation that passed the delegate's own map straight out would hand the supervisor an
     * object whose class — and therefore whose loader — it keeps alive for as long as it keeps
     * the map.
     */
    private static final String DETAILS_SOURCE = """
            package probe;

            import java.util.LinkedHashMap;

            public class ProbeDetails extends LinkedHashMap<String, String> {
            }
            """;

    /**
     * @param workDir a temp directory; the jar and intermediate class files are written here
     * @return path to the compiled jar
     */
    static Path compileToJar(Path workDir) throws IOException {
        Path classes = Files.createDirectories(workDir.resolve("probe-classes"));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException(
                    "No system Java compiler. These tests need a JDK, not a JRE.");
        }

        List<JavaFileObject> sources = List.of(
                new StringSource("probe.ProbeService", PROBE_SOURCE),
                new StringSource("probe.LazyProbe", LAZY_SOURCE),
                new StringSource("probe.ProbeDetails", DETAILS_SOURCE));

        // The SPI must be on the compile classpath, and it already is: this test module depends
        // on it, so the running JVM's classpath contains it.
        List<String> options = List.of(
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString());

        try (StandardJavaFileManager files = compiler.getStandardFileManager(null, null, null)) {
            boolean compiled = compiler.getTask(null, files, null, options, null, sources).call();
            if (!compiled) {
                throw new IllegalStateException("Failed to compile the probe service; see compiler output above.");
            }
        }

        Path jar = workDir.resolve("probe-service.jar");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar));
             Stream<Path> tree = Files.walk(classes)) {
            List<Path> classFiles = new ArrayList<>();
            tree.filter(p -> p.getFileName().toString().endsWith(".class")).forEach(classFiles::add);
            for (Path classFile : classFiles) {
                String entryName = classes.relativize(classFile).toString().replace('\\', '/');
                out.putNextEntry(new JarEntry(entryName));
                Files.copy(classFile, out);
                out.closeEntry();
            }
        }
        return jar;
    }

    private static final class StringSource extends SimpleJavaFileObject {
        private final String code;

        StringSource(String className, String code) {
            super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension),
                    Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }
}
