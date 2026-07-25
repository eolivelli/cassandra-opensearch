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
                }

                private volatile ServiceStatus status = ServiceStatus.NEW;
                private volatile String observedTccl = "<none>";
                private volatile boolean stopped = false;

                @Override
                public String name() { return "probe"; }

                @Override
                public void start(ServiceContext context) throws Exception {
                    status = ServiceStatus.STARTING;
                    observedTccl = String.valueOf(Thread.currentThread().getContextClassLoader().getName());
                    if ("true".equals(context.settings().get("probe.failOnStart"))) {
                        status = ServiceStatus.FAILED;
                        throw new ServiceException("probe", "probe was asked to fail",
                                new ProbeFailure("thrown from inside the isolated loader"));
                    }
                    status = ServiceStatus.RUNNING;
                }

                @Override
                public ServiceStatus status() { return status; }

                @Override
                public Map<String, String> details() {
                    Map<String, String> details = new LinkedHashMap<>();
                    details.put("name", name());
                    details.put("tccl", observedTccl);
                    details.put("loader", String.valueOf(getClass().getClassLoader().getName()));
                    details.put("stopped", String.valueOf(stopped));
                    return details;
                }

                @Override
                public void prepareDecommission(DecommissionContext context) {
                    status = ServiceStatus.DECOMMISSIONING;
                }

                @Override
                public boolean awaitDecommissionReady(DecommissionContext context) { return true; }

                @Override
                public void decommission(DecommissionContext context) {
                    status = ServiceStatus.DECOMMISSIONED;
                }

                @Override
                public void stop() {
                    stopped = true;
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
                new StringSource("probe.LazyProbe", LAZY_SOURCE));

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
