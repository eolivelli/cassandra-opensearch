/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

/**
 * Keeps Cassandra's default uncaught-exception handler from swallowing everybody else's exceptions.
 *
 * <p>{@code CassandraDaemon.setup()} calls
 * {@code Thread.setDefaultUncaughtExceptionHandler(JVMStabilityInspector::uncaughtException)}. That
 * setting is process-wide, so in a co-hosted JVM it silently takes ownership of OpenSearch's and
 * the supervisor's threads too — and {@code JVMStabilityInspector} does more than log: it inspects
 * the throwable for disk and commit-log failures and can ask the killer to end the process.
 *
 * <p>This handler restores the split. A thread is treated as Cassandra's when its context
 * ClassLoader is the isolated loader (or a child of it), which is true for every thread Cassandra
 * spawns because it inherits the context loader of the thread that created it, and the supervisor
 * pins that loader around {@code start()}. Everything else goes to whatever handler was installed
 * before Cassandra started, or to the JVM's default behaviour if there was none.
 */
final class DelegatingUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private final ClassLoader serviceLoader;
    private final Thread.UncaughtExceptionHandler cassandraHandler;
    private final Thread.UncaughtExceptionHandler previousHandler;

    DelegatingUncaughtExceptionHandler(ClassLoader serviceLoader,
                                       Thread.UncaughtExceptionHandler cassandraHandler,
                                       Thread.UncaughtExceptionHandler previousHandler) {
        this.serviceLoader = serviceLoader;
        this.cassandraHandler = cassandraHandler;
        this.previousHandler = previousHandler;
    }

    /** The handler that was in force before Cassandra replaced it; may be null. */
    Thread.UncaughtExceptionHandler previousHandler() {
        return previousHandler;
    }

    @Override
    public void uncaughtException(Thread thread, Throwable error) {
        if (belongsToService(thread)) {
            cassandraHandler.uncaughtException(thread, error);
        } else if (previousHandler != null) {
            previousHandler.uncaughtException(thread, error);
        } else {
            System.err.print("Exception in thread \"" + thread.getName() + "\" ");
            error.printStackTrace(System.err);
        }
    }

    private boolean belongsToService(Thread thread) {
        for (ClassLoader loader = thread.getContextClassLoader(); loader != null; loader = loader.getParent()) {
            if (loader == serviceLoader) {
                return true;
            }
        }
        return false;
    }
}
