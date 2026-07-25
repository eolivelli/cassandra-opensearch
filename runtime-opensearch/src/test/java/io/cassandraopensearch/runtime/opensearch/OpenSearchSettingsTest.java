/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import io.cassandraopensearch.spi.ServiceException;
import io.cassandraopensearch.spi.ServiceStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ObjectInputFilter;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the operator's {@code opensearch.yml} is and is not allowed to do to the settings the
 * {@code Node} is built from.
 */
class OpenSearchSettingsTest {

    private static final int HTTP_PORT = 19240;
    private static final int TRANSPORT_PORT = 19340;

    private static final String SERIAL_FILTER = "bootstrap.serial_filter";

    private final OpenSearchService service = new OpenSearchService();

    @AfterEach
    void stopService() throws Exception {
        service.stop();
    }

    /**
     * The layering regression. {@code InternalSettingsPreparer.prepareEnvironment} does not use
     * the builder it is handed as the base layer: it re-reads {@code <configDir>/opensearch.yml}
     * and puts the supplied settings on top, so anything removed from the builder beforehand is
     * put straight back by the file. The removal has to happen to the environment that comes out.
     *
     * <p>Asserted against {@code nodeEnvironment()} rather than a side effect, because that is
     * literally the object {@code start()} hands to the {@code Node} constructor.
     */
    @Test
    void bootstrapSerialFilterInTheOperatorsYamlNeverReachesTheNode(@TempDir Path home) throws Exception {
        TestServiceContext context = new TestServiceContext(home, "serial-filter-node", HTTP_PORT, TRANSPORT_PORT)
                .withConfigLine(SERIAL_FILTER + ": true");

        var environment = service.nodeEnvironment(context);

        assertThat(environment.settings().hasValue(SERIAL_FILTER))
                .as("a JVM-wide deserialization filter would break Cassandra in this process")
                .isFalse();
        assertThat(environment.settings().keySet()).doesNotContain(SERIAL_FILTER);
        assertThat(context.events())
                .anyMatch(event -> event.startsWith("WARN opensearch.setting.ignored")
                        && event.contains(SERIAL_FILTER));

        // Nothing else from the operator's file, and nothing the supervisor owns, is collateral.
        assertThat(environment.settings().get("discovery.type")).isEqualTo("single-node");
        assertThat(environment.settings().get("node.attr.embedded")).isEqualTo("true");
        assertThat(environment.settings().get("http.port")).isEqualTo(Integer.toString(HTTP_PORT));
        assertThat(environment.settings().get("node.name")).isEqualTo("serial-filter-node");
        assertThat(environment.configDir()).isEqualTo(context.configDirectory());
        assertThat(environment.logsDir()).isEqualTo(context.logsDirectory());
        assertThat(environment.dataFiles()).containsExactly(context.dataDirectory());
    }

    /** A file with no such key must not produce the warning, or it stops meaning anything. */
    @Test
    void anOrdinaryConfigurationIsReportedAsNothingIgnored(@TempDir Path home) throws Exception {
        TestServiceContext context = new TestServiceContext(home, "plain-node", HTTP_PORT, TRANSPORT_PORT);

        service.nodeEnvironment(context);

        assertThat(context.events()).noneMatch(event -> event.contains("opensearch.setting.ignored"));
    }

    /**
     * The same thing end to end. {@code opensearch-3.7.0} declares no {@code serial_filter}
     * setting — {@code BootstrapSettings} has only {@code SECURITY_FILTER_BAD_DEFAULTS},
     * {@code MEMORY_LOCK}, {@code SYSTEM_CALL_FILTER} and {@code CTRLHANDLER} — so a key that
     * survives the layering is an {@code unknown setting} failure and the node never starts.
     */
    @Test
    void aNodeStartsDespiteBootstrapSerialFilterInTheOperatorsYaml(@TempDir Path home) throws Exception {
        TestServiceContext context = new TestServiceContext(home, "serial-filter-start", HTTP_PORT, TRANSPORT_PORT)
                .withConfigLine(SERIAL_FILTER + ": true");

        service.start(context);

        assertThat(service.status()).isEqualTo(ServiceStatus.RUNNING);
        assertThat(new Rest(HTTP_PORT).get("/").status()).isEqualTo(200);
        assertThat(ObjectInputFilter.Config.getSerialFilter())
                .as("no process-wide deserialization filter may be installed; Cassandra's JMX/RMI"
                        + " and internal serialization run in this same JVM")
                .isNull();
    }

    /**
     * After a failed start there is no node, so {@code details()} must not go on advertising one.
     * The identity fields are filled in one at a time as {@code start()} gets further, and a
     * FAILED service reporting a name, an id and two listen addresses reads as a node that is
     * there and merely unhealthy — while nothing is listening on either address.
     */
    @Test
    void detailsAfterAFailedStartAdvertiseNoNode(@TempDir Path home) {
        TestServiceContext context = new TestServiceContext(home, "doomed-node", HTTP_PORT, TRANSPORT_PORT)
                .withConfigLine("this.setting.does.not.exist: true");

        assertThatThrownBy(() -> service.start(context)).isInstanceOf(ServiceException.class);

        assertThat(service.status()).isEqualTo(ServiceStatus.FAILED);
        assertThat(service.details())
                .containsExactly(java.util.Map.entry("status", "FAILED"));
    }
}
