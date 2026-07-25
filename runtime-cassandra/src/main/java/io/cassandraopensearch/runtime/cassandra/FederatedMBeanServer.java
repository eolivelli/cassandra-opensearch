/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.cassandra;

import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * A read-only union of the node's private {@link MBeanServer} and the JVM platform server.
 *
 * <p>The node deliberately owns a private MBeanServer so that {@code org.apache.cassandra:*} never
 * appears in the platform namespace that OpenSearch and the supervisor also use. That alone breaks
 * two things, both of which the spike hit and neither of which Cassandra's own {@code dtest}
 * framework sees:
 *
 * <ul>
 *   <li><b>{@code nodetool} cannot connect usefully.</b> {@code NodeProbe.connect()} immediately
 *       builds proxies for {@code java.lang:type=Memory} and {@code java.lang:type=Runtime}, and
 *       {@code nodetool info} reads heap and uptime from them. A bare private server has neither.</li>
 *   <li><b>{@code GCInspector} throws on every GC.</b> Its constructor populates {@code gcStates}
 *       from {@code MBeanWrapper.instance.queryNames("java.lang:type=GarbageCollector,*")} but
 *       registers its notification listener on the <i>platform</i> server. Against a bare private
 *       server the query is empty, so every subsequent GC notification dereferences a null
 *       {@code GCState}. dtest never calls {@code GCInspector.register()}; a real
 *       {@code CassandraDaemon.setup()} does.</li>
 * </ul>
 *
 * <p>Routing is by domain: the JDK's own domains resolve against the platform server, everything
 * else against the node's. Wildcard queries return the union. Writes are not routed at all — the
 * only entry points that mutate are reached through {@link NodeMBeanWrapper}, which always writes
 * to the private server, so no Cassandra MBean can leak into the platform namespace by accident.
 *
 * <p>Implemented as a dynamic proxy purely to avoid hand-writing forty {@code MBeanServer} methods.
 */
final class FederatedMBeanServer {

    private FederatedMBeanServer() {
    }

    static MBeanServer federate(MBeanServer nodeServer) {
        MBeanServer platform = ManagementFactory.getPlatformMBeanServer();
        InvocationHandler handler = (proxy, method, args) -> {
            String name = method.getName();
            try {
                if (("queryNames".equals(name) || "queryMBeans".equals(name)) && isWildcard(args)) {
                    Set<Object> union = new LinkedHashSet<>();
                    union.addAll((Set<?>) method.invoke(nodeServer, args));
                    union.addAll((Set<?>) method.invoke(platform, args));
                    return union;
                }
                return method.invoke(route(nodeServer, platform, args), args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (MBeanServer) Proxy.newProxyInstance(
                FederatedMBeanServer.class.getClassLoader(), new Class<?>[]{MBeanServer.class}, handler);
    }

    private static boolean isWildcard(Object[] args) {
        if (args == null || args[0] == null) {
            return true;
        }
        ObjectName pattern = (ObjectName) args[0];
        return pattern.isDomainPattern() || pattern.getDomain().isEmpty();
    }

    private static MBeanServer route(MBeanServer nodeServer, MBeanServer platform, Object[] args) {
        if (args != null) {
            for (Object arg : args) {
                if (arg instanceof ObjectName) {
                    return isPlatformDomain(((ObjectName) arg).getDomain()) ? platform : nodeServer;
                }
            }
        }
        return nodeServer;
    }

    private static boolean isPlatformDomain(String domain) {
        return domain.startsWith("java.")
                || domain.startsWith("javax.")
                || domain.startsWith("jdk.")
                || domain.startsWith("com.sun.management")
                || domain.equals("JMImplementation");
    }
}
