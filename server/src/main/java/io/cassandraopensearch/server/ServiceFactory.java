/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import io.cassandraopensearch.bootstrap.ClasspathResolver;
import io.cassandraopensearch.bootstrap.IsolatedService;
import io.cassandraopensearch.server.config.ServiceConfiguration;
import io.cassandraopensearch.spi.EmbeddedService;

/**
 * Turns a {@link ServiceConfiguration} into a live {@link EmbeddedService}.
 *
 * <p>This is the only seam between the supervisor and the ClassLoader machinery, and it exists
 * so the supervisor's own tests can drive the coupled lifecycle with fakes. Booting a real
 * Cassandra or OpenSearch to assert that shutdown runs in reverse order would test the servers,
 * not the ordering — that belongs in {@code integration-tests}, against the assembled tarball.
 */
@FunctionalInterface
public interface ServiceFactory {

    EmbeddedService create(ServiceConfiguration configuration) throws Exception;

    /**
     * The production factory: one {@code IsolatedClassLoader} per service, built from the jars
     * in {@code lib/<service>/}.
     *
     * <p>Note what is absent — any mention of {@code CassandraService} or {@code
     * OpenSearchService} as a type. They are named by string in {@code
     * ServiceConfiguration.implementationClass()} and resolved inside the child loader; a static
     * reference here would put them on the supervisor's classpath and collapse the isolation.
     */
    static ServiceFactory isolated() {
        return configuration -> IsolatedService.create(
                configuration.name(),
                configuration.implementationClass(),
                ClasspathResolver.jarsIn(configuration.libDirectory()));
    }
}
