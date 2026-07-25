# Known gaps

Things that are not finished, not safe, or not what the rest of the documentation implies. Kept
in one place so nobody has to discover them by being surprised in production.

## 1. `watch_external_decommission` narrows the unsupervised path; it does not make it safe

**What it does now.** The Cassandra runtime polls `StorageService.getOperationMode()` once a
second on a daemon thread and reports `ring.state.changed` whenever it moves. When that says the
node is leaving the ring — `LEAVING`, which a bare `nodetool decommission` produces, or
`DECOMMISSIONED` — and the supervisor is not running a decommission of its own, it logs a WARN
naming the situation and calls `prepareDecommission` on OpenSearch, which excludes this node from
shard allocation so the cluster starts relocating. The setting turns that off.

**What it still cannot do.** By the time Cassandra reports `LEAVING` the ring transition has
already begun, so this is a catch-up that may lose the race, and on a node holding real index data
it will. The supported path excludes OpenSearch *before* Cassandra is touched at all and then
blocks until the last shard has moved; this one starts the relocation and has no way to hold
anything up while it finishes — `nodetool` is not waiting for it, and the supervisor has no say in
when the operator stops the process.

There is more grace than the shape of the problem suggests, and it is worth knowing: Cassandra's
`decommission()` sets `LEAVING` and then sleeps `RING_DELAY_MILLIS` (30 s) before it streams
anything, and when it finishes it leaves the process running — "let op be responsible for killing
the process". So the node keeps serving search while the shards move. **The exposure is the
window between the operator's `nodetool decommission` returning and the operator stopping the
process**, and nothing in this product controls it. Watch `_cat/shards` and let the node empty
before stopping it.

The supervisor deliberately stops at the exclusion. It does not wait for relocation and then stop
OpenSearch itself: the wait would run for up to `shard_relocation_timeout` on a background thread
while `bin/cassandra-opensearch decommission`, the shutdown hook and `stop` all remain able to
drive the same service, which is the concurrent lifecycle the SPI forbids — and stopping
OpenSearch early would destroy exactly the shards the exclusion exists to save, while stopping it
after relocation has finished achieves nothing a still-running node does not.

`bin/cassandra-opensearch decommission` remains the only ordering-safe way to retire a node.

## 2. Three Cassandra settings are documentation, not configuration

`services.cassandra.native_transport_port`, `storage_port` and `listen_address` are accepted by
the YAML parser and reach nothing. Cassandra takes all three from `conf/cassandra.yaml`, which it
parses itself. They are retained because they record what must match — but they must match by
hand. Changing a port means changing `cassandra.yaml`.

This is the one place the config model knowingly violates its own rule that a setting which has
no effect is worse than no setting at all.

## 3. `ThreadAwareSecurityManager` is a JDK 24 blocker

Cassandra calls `System.setSecurityManager` process-wide from
`ThreadAwareSecurityManager.install()`. On JDK 21 this needs `-Djava.security.manager=allow` and
prints a terminal-deprecation warning on every boot. **The API is removed entirely in JDK 24**, so
this stack cannot move past 21 without a fork patch making `install()` skippable. It exists only
for UDF sandboxing, and `user_defined_functions_enabled` already defaults to false.

## 4. Neither isolated ClassLoader can be garbage collected after stop

Confirmed for both services. OpenSearch: a `WeakReference` survived 40 forced GC cycles across
four configurations, with the retaining root unidentified — no shutdown hook, live thread, TCCL or
`ThreadLocal` referenced it, so it is most likely a static in a `java.base` class. Cassandra:
about 11 daemon threads per node keep it rooted, two of which have **no fix available from
outside the fork** — JNA's per-loader `Cleaner`, and `nodes-info-persistence`, created by the
fork-specific `org.apache.cassandra.nodes.Nodes`, which exposes no shutdown API at all.

Irrelevant as shipped: the process loads each service exactly once. It becomes a metaspace leak
the moment anything recreates a loader, so **restart a service in place inside its existing
loader** rather than rebuilding the loader.

## 5. Cassandra reads unnamespaced system properties

`ssl.enable`, `default.*`, `compaction_rate_limit_granularity_in_kb` and
`check_data_resurrection_heartbeat_period_milli` have no prefix at all. A plain `-Dssl.enable=true`
set for any unrelated reason silently turns on Cassandra's JMX SSL client mode. Cassandra also
reads `log4j2.disable.jmx` and `log4j.shutdownHookEnabled`, which OpenSearch's log4j2 *uses*.

Nothing in this project sets them; the risk is whatever else shares the command line.

## 6. Netty tuning is a shared, unreconciled budget

`io.netty.*` properties are JVM-global and read by both Netty copies, and the two servers want
opposite things: Cassandra's options want `allocator.maxOrder=11` and
`tryReflectionSetAccessible=true`, while the OpenSearch distribution wants `noUnsafe=true` and
`numDirectArenas=0`. There is no setting that satisfies both.

**The conflict is resolved in Cassandra's favour, and OpenSearch pays for it.**
`conf/jvm21-server.options` ships three properties:

```
-Dio.netty.tryReflectionSetAccessible=true
-Dio.netty.allocator.useCacheForAllThreads=true
-Dio.netty.allocator.maxOrder=11
```

The launcher puts every line of that file on the JVM command line, so all three apply to
OpenSearch's Netty as well. `maxOrder=11` quadruples Netty's arena chunk size to 16 MiB, which is a
real footprint change for a server that was not tuned for it. OpenSearch's own preferences are
deliberately **not** adopted, because `noUnsafe=true` measurably slows Cassandra's native transport.

An earlier version of this section claimed the project set none of them. It was wrong, and the
error mattered: this is the file an operator sizing direct memory would consult. The Java code in
both runtimes does set none — the properties come from the shipped options file — which is how the
two statements drifted apart.

Related: `MaxDirectMemorySize` is one budget that each Netty tracks independently, so the two can
collectively over-commit it. Size for the sum.

Related: `MaxDirectMemorySize` is one budget that each Netty tracks independently, so the two can
collectively over-commit it. Size for the sum.

## 7. Memtable accounting is approximate

Stock Cassandra runs with `-javaagent:lib/jamm-*.jar`, which `cassandra-env.sh` adds and which we
cannot reproduce per-service — a `-javaagent` applies to the whole JVM, so it would attach to
OpenSearch too. Without it, `ObjectSizes` falls back to the specification strategy and memtable
size accounting is approximate. If exact accounting matters, the agent has to go on the host
command line, accepting that it instruments both servers.

## 8. Startup that overruns its timeout is abandoned, not killed

A `start()` exceeding its bound is interrupted and left behind — nothing in the JVM can kill a
thread outright. The supervisor's thread is a daemon and the ordered shutdown still runs, but a
runtime that had already spawned non-daemon threads before hanging could hold the JVM open.

## 9. Nothing in the shipped configuration is authenticated

Called out because this file exists so nobody meets a surprise in production, and a reviewer found
it recorded nowhere:

- The supervisor's JMX connector runs with `authenticate=false` and `ssl=false`.
- Cassandra's own JMX connector sets no authenticator.
- The shipped `conf/cassandra.yaml` uses `AllowAllAuthenticator` and `AllowAllAuthorizer`.
- OpenSearch ships with no security plugin.

Every one of these is the stock upstream default, and every endpoint binds loopback only in the
shipped configuration, so a default single-node install is not exposed. But the moment an operator
changes `listen_address`, `rpc_address` or `network_host` to reach the node from another machine —
which the multi-node instructions require — they are exposing unauthenticated CQL, unauthenticated
JMX with remote code execution reachable through it, and an unauthenticated OpenSearch REST API.

Nothing in this project hardens any of that, and nothing warns at startup.

## 10. A transient allocation exclusion can shadow a persistent one

`prepareDecommission` merges this node into `cluster.routing.allocation.exclude._name` at the
**transient** level, and reads the existing list from `transientSettings()`. If an operator has set
a *persistent* exclusion for a different node, the transient value takes precedence for that key
while the decommission is in force, so the persistent entry is shadowed until `abortDecommission`
or the node's departure removes ours.

The alternative — reading the merged view — is worse: it copies persistent entries into the
transient layer permanently, so dropping the persistent setting later would leave a transient
shadow keeping that node excluded forever. Both readings are imperfect; a correct one needs a
compare-and-set the cluster settings API does not offer.

## 11. The supervisor MBean is not on Cassandra's JMX port

`bin/cassandra-opensearch status|decommission` reach `io.cassandraopensearch:type=Supervisor` via
the launcher's `com.sun.management.jmxremote.*` agent, on its own port. Port 7199 will not serve
it: Cassandra's federated MBeanServer routes every non-JDK domain to its private server. Two JMX
endpoints is the cost of keeping Cassandra's MBeans out of the platform namespace.
