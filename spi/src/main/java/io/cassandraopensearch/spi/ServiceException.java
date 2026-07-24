/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.spi;

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
     * Rebuilds a throwable's cause chain out of plain {@link RuntimeException}s carrying the
     * original type name and stack trace, so it can be thrown into a ClassLoader that cannot
     * load the original exception classes.
     */
    public static Throwable flatten(Throwable cause) {
        if (cause == null) {
            return null;
        }
        Throwable flattenedCause = flatten(cause.getCause());
        RuntimeException flattened =
                new RuntimeException(cause.getClass().getName() + ": " + cause.getMessage(), flattenedCause);
        flattened.setStackTrace(cause.getStackTrace());
        return flattened;
    }
}
