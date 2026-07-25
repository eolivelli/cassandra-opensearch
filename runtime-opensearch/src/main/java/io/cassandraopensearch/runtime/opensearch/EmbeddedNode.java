/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.runtime.opensearch;

import org.opensearch.Version;
import org.opensearch.env.Environment;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;

import java.util.Collection;
import java.util.List;

/**
 * The {@link Node} we actually instantiate.
 *
 * <p>{@code Node}'s only usable constructor is {@code protected} — the public one goes through
 * {@code PluginsService} scanning {@code modules/} and {@code plugins/} on disk, which we must
 * not do (see below) — so it has to be subclassed. This is exactly what {@code MockNode} in the
 * OpenSearch test framework does, and it is the supported way to embed a node.
 *
 * <p>Note that {@code Node.validateNodeBeforeAcceptingRequests} is empty in the base class:
 * bootstrap checks are run only by {@code org.opensearch.bootstrap.Bootstrap}, which embedding
 * bypasses entirely. Nothing has to be disabled to bind a non-loopback address here.
 */
final class EmbeddedNode extends Node {

    EmbeddedNode(Environment environment, Collection<PluginInfo> classpathPlugins) {
        // forbidPrivateIndexSettings=true is the production behaviour; only the test framework
        // relaxes it, to be able to create indices pinned to old versions.
        super(environment, classpathPlugins, true);
    }

    /**
     * Describes a plugin that is already on this ClassLoader's classpath.
     *
     * <p>Classpath plugins are the whole reason {@code modules/} and {@code plugins/} stay
     * absent from disk. For every bundle it finds in those directories {@code PluginsService}
     * runs a jar-hell check against {@code System.getProperty("java.class.path")} — which under
     * ClassLoader isolation is the <i>supervisor's</i> classpath, not OpenSearch's. That check
     * would both miss real conflicts and invent imaginary ones. A {@code PluginInfo} handed
     * directly to the constructor skips that path.
     *
     * <p>{@code transport-netty4} is a module rather than a core component, so without this the
     * node comes up with no HTTP layer at all and the REST API simply does not exist.
     */
    static PluginInfo classpathPlugin(Class<? extends Plugin> type) {
        return new PluginInfo(
                type.getName(),
                "classpath plugin",
                "NA",
                Version.CURRENT,
                "21",
                type.getName(),
                null,
                List.of(),
                false);
    }
}
