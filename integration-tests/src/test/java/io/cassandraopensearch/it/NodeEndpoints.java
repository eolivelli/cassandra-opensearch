/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

/**
 * Every address and port an IT node listens on, decided in one place.
 *
 * <p>Nothing here uses a stock port. A developer machine or a CI agent very often already has
 * something on 9042 and 9200, and if that something is a container published on {@code 0.0.0.0}
 * then <b>every</b> loopback alias is taken with it — moving the node to 127.0.0.2 does not help
 * and the failure arrives as an opaque "address already in use" from inside a child ClassLoader.
 * The 21xxx block is picked to be improbable rather than merely unused today.
 *
 * <p>The two nodes differ in both address and port. Differing addresses alone would be enough for
 * Cassandra — that is what {@code ccm} does — but the JDK's management agent, Cassandra's JMX
 * connector and OpenSearch's HTTP transport each bind on their own terms, and a shared port that
 * one of them decides to bind on the wildcard address turns into a cross-node collision that
 * looks like a node simply failing to start.
 *
 * @param host              listen address; distinct per node, from the 127.0.0.0/8 block Linux
 *                          routes to {@code lo} without any alias configuration
 * @param storagePort       Cassandra's gossip and streaming port ({@code storage_port})
 * @param sslStoragePort    kept distinct even though TLS is off, because Cassandra binds it
 * @param nativePort        CQL ({@code native_transport_port})
 * @param cassandraJmxPort  the embedded node's own JMX connector, which {@code bin/nodetool} uses
 * @param supervisorJmxPort the JDK management agent carrying
 *                          {@code io.cassandraopensearch:type=Supervisor}, which
 *                          {@code bin/cassandra-opensearch status|stop|decommission} uses.
 *                          Deliberately not the same endpoint: Cassandra's federated MBean server
 *                          routes every non-JDK domain to its private server, so the supervisor
 *                          MBean is not reachable there. See docs/KNOWN-GAPS.md §9.
 * @param httpPort          OpenSearch REST
 * @param transportPort     OpenSearch node-to-node transport
 */
record NodeEndpoints(
        String host,
        int storagePort,
        int sslStoragePort,
        int nativePort,
        int cassandraJmxPort,
        int supervisorJmxPort,
        int httpPort,
        int transportPort) {

    static final NodeEndpoints FIRST =
            new NodeEndpoints("127.0.0.1", 21100, 21101, 21142, 21199, 21299, 21200, 21300);

    static final NodeEndpoints SECOND =
            new NodeEndpoints("127.0.0.2", 21110, 21111, 21152, 21209, 21309, 21210, 21310);

    /** {@code host:port} as Cassandra's {@code seed_provider} and gossip write it. */
    String storageAddress() {
        return host + ':' + storagePort;
    }

    /** {@code host:port} as OpenSearch's {@code TransportAddress.toString()} renders it. */
    String transportAddress() {
        return host + ':' + transportPort;
    }
}
