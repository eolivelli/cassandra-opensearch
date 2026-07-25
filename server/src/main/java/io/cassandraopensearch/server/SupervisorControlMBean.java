/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server;

import java.util.Map;

/**
 * The management interface of a running process, registered on the platform MBean server as
 * {@code io.cassandraopensearch:type=Supervisor}.
 *
 * <p>This is how {@code bin/cassandra-opensearch status} and {@code bin/cassandra-opensearch
 * decommission} talk to a node they did not start. JMX rather than a REST endpoint because the
 * supervisor must stay reachable when OpenSearch's HTTP transport is exactly the thing that is
 * broken, and because {@code nodetool} already establishes JMX as this product's control plane.
 *
 * <p>Every type crossing this interface is a JDK type. A remote client holds only the JDK and
 * this interface; anything else would have to be on its classpath too.
 */
public interface SupervisorControlMBean {

    /** Overall process state — one of the {@link SupervisorState} names. */
    String getState();

    /** Why the process is going down, when it is going down unhappily; empty otherwise. */
    String getFailureMessage();

    String getClusterName();

    /** Started services, in startup order. */
    String[] getServiceNames();

    /** Each service's {@link io.cassandraopensearch.spi.ServiceStatus}, keyed by service name. */
    Map<String, String> getServiceStatuses();

    /**
     * Diagnostics for one service: cluster name, node id, listen addresses, ring or shard state.
     * The contents are the service's to define — see {@code EmbeddedService.details()}.
     */
    Map<String, String> serviceDetails(String service);

    /**
     * The last thing a decommission reported about itself, for a CLI to render while its own
     * {@link #decommission} call is still blocked.
     */
    String getDecommissionProgress();

    /**
     * Retires this node from both clusters and stops the process — the operation behind {@code
     * bin/cassandra-opensearch decommission}.
     *
     * <p>Blocks for the duration of the whole procedure, which can legitimately be an hour. Poll
     * {@link #getDecommissionProgress()} from another connection to follow it.
     *
     * @param timeoutSeconds bound on each waiting phase; 0 to use the configured limits
     * @param force          proceed with shards outstanding, accepting that any shard with no
     *                       other copy is lost
     * @return a one-line summary of what happened
     * @throws DecommissionException if a phase failed or gave up; the message says which, and
     *                               what state the node was left in
     */
    String decommission(long timeoutSeconds, boolean force) throws DecommissionException;
}
