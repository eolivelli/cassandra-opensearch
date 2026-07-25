/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

/**
 * Prints every live node's logs when a test fails.
 *
 * <p>The assertion message can only ever say what the test saw from outside — a refused
 * connection, a missing row, a 400. What the node did is in {@code logs/}, in a temporary
 * directory that is gone by the time anyone reads the build output, so it is dumped here while it
 * still exists.
 */
public final class DumpLogsOnFailure implements TestWatcher {

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        DistributionNode.dumpAllLive("test failed: " + context.getDisplayName());
    }
}
