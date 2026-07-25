# Spike: Cassandra in an isolated ClassLoader on JDK 21

Findings from the de-risking spike. Two agents ran this independently and agreed on every
material point; where they differ the more thoroughly evidenced result is recorded. Working code
is in the session scratchpad under `spike-cassandra/` and `spike2/`.

**Verdict: it works.** A real node boots in a parent-last child ClassLoader in ~10 s and reports
`mode=NORMAL`; stock `bin/nodetool` manages it over JMX; CQL round-trips from a driver in the
*application* loader; two nodes run in one JVM and decommission works between them; the JVM exits
0. Six caveats below, none of them blockers, several of which need code in this project.

## The shared-class predicate: do not copy dtest's

This is the most important lesson and it invalidates the obvious approach. dtest's
`InstanceClassLoader` decides what to share with the parent by **package prefix** — `javax.`,
`sun.`, `com.sun.` and friends. That is safe *only* because dtest's parent loader already holds
the entire Cassandra classpath, so a prefix miss silently resolves anyway.

Our supervisor deliberately holds none of Cassandra's dependencies, so the same predicate fails:

```
java.lang.NoClassDefFoundError: javax/annotation/Nullable
    at isolated-cassandra//org.apache.cassandra.config.YamlConfigurationLoader$PropertiesChecker
Caused by: java.lang.ClassNotFoundException: javax.annotation.Nullable
    at jdk.internal.loader.ClassLoaders$AppClassLoader.loadClass
```

`javax.annotation.Nullable` is not a JDK class — it is **jsr305**. The same trap catches
`com.sun.jna.*` (JNA) and `javax.inject.*`. The JDK shares these namespaces with third-party
jars, so a name prefix cannot answer "is this a platform class".

The fix is to *ask* the platform ClassLoader rather than pattern-match a name. **Our
`IsolatedClassLoader` gets this right structurally**: its parent *is* the platform loader, so
ordinary delegation asks the JDK first and falls through to our own jars when the JDK does not
have the class. `javax.management.MBeanServer` resolves from the platform; `javax.annotation.Nullable`
falls through to jsr305. No name list is involved, so there is nothing to get wrong.

## Isolation, measured

```
cassandra classpath entries: 116
child   StorageService          -> IsolatedClassLoader{cassandra}
app classloader sees org.apache.cassandra.* : false
child   io.netty.channel.EventLoopGroup -> IsolatedClassLoader{cassandra}
app     io.netty.channel.EventLoopGroup -> AppClassLoader        (the CQL driver's netty)
netty distinct across loaders: true
```

The CQL driver running in the application loader printed `SLF4J(W): No SLF4J providers were
found` — proof that logback exists only inside the child. Cassandra's netty 4.1.135 and the
driver's netty 4.1.94 ran simultaneously without interference.

## Startup sequence

```java
Thread.currentThread().setContextClassLoader(childLoader);   // Cassandra uses the TCCL

// Cassandra calls System.exit(100) from JVMStabilityInspector.Killer on OOM, FSError and
// disk_failure_policy=die. In a shared JVM that kills OpenSearch too.
JVMStabilityInspector.replaceKiller(interceptingKiller);

// runManaged=true makes CassandraDaemon.exitOrFail() throw instead of calling System.exit.
daemon = new CassandraDaemon(true);
daemon.activate();                     // applyConfig + registerNativeAccess + setup + start

// StorageService.initServer() registers a JVM-GLOBAL drain-on-shutdown hook. Remove it.
JVMStabilityInspector.removeShutdownHooks();
```

`activate()` is used rather than dtest's hand-rolled 40-step `setup()` replica: dtest reimplements
it only because it wants fake messaging, gossip and snitch, whereas we want a genuine node.

Two adjustments are mandatory. `runManaged=true`, as above — and **`-Dcassandra-foreground=yes`**,
without which `activate()` calls `System.out.close()` and `System.err.close()` and the entire
process goes silent.

### `System.exit` has two paths, not one

`runManaged=true` covers only `CassandraDaemon.exitOrFail`. `JVMStabilityInspector.Killer.killJVM()`
calls `System.exit(100)` independently, on OOM, `FSError`, corrupt sstables, and
`disk_failure_policy`/`commit_failure_policy` of `die`. It must be replaced via
`JVMStabilityInspector.replaceKiller(...)` **before** `activate()`. Missing this is a latent
whole-process kill.

## JVM flags for JDK 21

The fork ships `jvm17-server.options` and `jvm22-server.options` and nothing for 18–21. Bisecting
from zero flags produced this **minimum** for boot + CQL + JMX, each entry justified by the exact
failure that appears without it:

```
--add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED     # JmxRegistry -> sun.rmi.registry.RegistryImpl
--add-exports java.base/sun.nio.ch=ALL-UNNAMED          # FileUtils -> DirectBuffer.cleaner
--add-opens   java.base/sun.nio.ch=ALL-UNNAMED
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED    # FileUtils -> Cleaner.clean
--add-opens   java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens   java.base/java.io=ALL-UNNAMED             # FileDescriptor.fd
-Djava.security.manager=allow                           # ThreadAwareSecurityManager.install()
```

The full proposed file is shipped at `dist/src/main/resources/conf/jvm21-server.options`; it is
the 17-era set plus `-Djava.security.manager=allow`, and it **drops**
`--add-opens java.base/jdk.internal.util.jar` because that package no longer exists in JDK 21 and
keeping it prints a warning on every start.

Flags `conf/cassandra-env.sh` adds that we cannot reproduce when embedded:
`-javaagent:lib/jamm-0.4.0.jar` is the notable one. Without it memtable size accounting falls back
to the specification strategy and is **approximate**. If exact accounting matters, the agent has
to go on the host JVM command line — a real hosting constraint, since it would then apply to
OpenSearch too.

## JMX and stock nodetool

`nodetool` needs more than a private MBeanServer. Two problems had to be solved:

1. **`nodetool info` fails** against a bare private server: `NodeProbe.connect()` immediately
   fetches the platform `MemoryMXBean` and `RuntimeMXBean`, which a private server does not have.
2. **`GCInspector` throws an NPE on every GC.** Its constructor builds `gcStates` by querying
   `MBeanWrapper.instance`, but registers its listener on the *platform* server. With a bare
   private server `gcStates` stays empty and every GC produces
   `NullPointerException: Cannot read field "assumeGCIsPartiallyConcurrent"`. Reproduced three
   times in a single 10-second startup. dtest never hits this because it does not call
   `GCInspector.register()`.

Both are fixed by a **federated** MBeanServer: a proxy that routes `java.*`, `javax.*`, `jdk.*`,
`com.sun.management` and `JMImplementation` to the JVM platform server and everything else to the
node's private server, with `queryNames`/`queryMBeans` returning the union. Writes always go to
the private server, so no Cassandra MBean ever lands in the platform namespace and OpenSearch's
MBeans are untouched. It is installed through the supported extension point
`-Dorg.apache.cassandra.mbean_registration_class=...`, not a dtest-only flag.

One further dtest detail to *avoid*: `IsolatedJmx` passes a custom `RMIClientSocketFactory`, which
gets serialised into the RMI stub and shipped to the client. A remote `nodetool` would fail with
`ClassNotFoundException`; dtest never noticed because its nodetool is in-JVM. Use the JDK default
client socket factory.

Captured, from the real stock `bin/nodetool`:

```
UN  127.0.0.1  140.73 KiB  4  100.0%  77f3f201-6765-4f11-9cdd-8be58c5f8c9e  rack1

ID                     : 77f3f201-6765-4f11-9cdd-8be58c5f8c9e
Gossip active          : true
Native Transport active: true
Bootstrap state        : COMPLETED
```

`status`, `info`, `version`, `describecluster`, `gossipinfo`, `netstats`, `tpstats` all exit 0.

### The launcher scripts reject JDK 21

`bin/cassandra.in.sh` branches on 11, 17 and ≥22 with nothing in between, and string-compares
versions:

```sh
elif [ "$JVM_VERSION" \< "22" ] ; then echo "DSE DB 5.0 requires Java 11 or higher."; exit 1;
```

`"21.0.11" < "22"` is true, so **every JDK from 18 to 21 is rejected** with a misleading message,
killing `nodetool`, `cqlsh`, `cassandra` and `sstable*`. The server jar itself is unaffected.

The spike worked around it through the script's own `CASSANDRA_INCLUDE` hook without modifying the
fork. Our `cli` module must do the same. **This is also a fix the fork should take upstream.**

## Decommission

- **One node: impossible.** `no other normal nodes in the ring; decommission would be pointless`,
  and `force` does *not* help — the guard is evaluated before the flag is considered. The node
  stays `NORMAL`. The single-node equivalent of a clean teardown is `drain()` plus per-service
  shutdown.
- **Two nodes: works.** Two isolated loaders on 127.0.0.1 and 127.0.0.2, one JVM. Node 2 really
  bootstrapped and streamed (`Bootstrap completed for tokens [...]`), both nodes saw each other,
  and stock nodetool reported the ring correctly from either JMX port.
- On a 2-node ring plain decommission is refused for a *useful* reason — `Not enough live nodes to
  maintain replication factor in keyspace system_distributed (RF = 3, N = 2)` — and succeeds with
  `--force`, after which the ring converges to one node and `mode = DECOMMISSIONED`.
- The failure state is `DECOMMISSION_FAILED`, not a revert to `NORMAL`.

A genuine decommission integration test therefore needs **two nodes**, and either `--force` or
lowered replication factors on `system_distributed`/`system_auth`. Three nodes would exercise the
non-forced path.

Constraints on a two-node in-JVM test:
- Distinct loopback listen addresses; ports may repeat, as `ccm` does. macOS needs
  `ifconfig lo0 alias 127.0.0.2` first.
- **JMX must differ by port, not address.** `java.rmi.server.hostname` is read once and cached in
  RMI's static `TCPEndpoint.localEndpoints`, so two RMI servers in one JVM cannot advertise
  different hostnames.
- **Startup must be sequential.** `DatabaseDescriptor.daemonInitialization()` snapshots the
  JVM-global `cassandra.config` and `cassandra.storagedir` per loader at boot, so the properties
  must be rewritten between boots. Parallel boot is impossible without changing Cassandra.
- Each node needs a distinct MBeanServer agent id.

## Shutdown

This sequence produced `JVM-EXIT-CODE=0`:

```
destroyClientTransports -> drain -> stop JMX -> MessagingService.shutdown
-> Stage / ScheduledExecutors / ColumnFamilyStore / BufferPools / Ref / SSTableReader
-> SharedExecutorPool -> CommitLog.shutdownBlocking -> child LoggerContext.stop -> loader.close
```

Ordering matters: closing the MBean wrapper *before* tearing down executors made every subsequent
step fail, because Cassandra keeps registering and unregistering metrics MBeans throughout its own
shutdown. The wrapper must become a **no-op after close**, not null out its fields.

Only one thread outlives shutdown — netty's `GlobalEventExecutor` `TaskRunner`, non-daemon, which
self-terminates after ~1 s idle and does not block JVM exit.

**The ClassLoader is not collectable after stop.** Roughly 11 daemon threads per node keep it
rooted. Most map onto shutdown calls that dtest's `Instance.shutdown()` makes; two have **no fix
available from outside the fork**:

- `JNA Cleaner` — JNA creates one per loader and never stops it.
- `nodes-info-persistence` — created by the fork-specific `org.apache.cassandra.nodes.Nodes`,
  which **exposes no shutdown API at all** and does not exist upstream, so dtest does not cover it.
  **The fork needs a `Nodes.shutdown()`.**

Since this project loads Cassandra once per process, the practical impact is nil. It matters only
if the loader is ever recreated.

## Co-hosting with OpenSearch

**Native libraries are fine — measured, not assumed.** Two isolated loaders over the same 116 jars
each loaded JNA, netty-epoll, netty-tcnative and Cassandra's `INativeLibrary` successfully. Each
extracts its `.so` to a uniquely named temp file per loader and `System.load()`s that path, and
the JVM's "already loaded in another classloader" rule is per *file*. The one thing that would
break it is pointing both at a shared path via `-Djna.boot.library.path` or `jna.nosys=false`.

**The real risk is JVM-global system properties.** These cannot be scoped per loader:

- Cassandra's own options set `io.netty.tryReflectionSetAccessible=true`,
  `io.netty.allocator.useCacheForAllThreads=true` and `io.netty.allocator.maxOrder=11`. **OpenSearch's
  netty reads all three.** `maxOrder=11` quadruples netty's arena chunk size to 16 MiB — a real
  footprint change for OpenSearch. Conversely the OpenSearch distribution's `io.netty.noUnsafe=true`
  and `numDirectArenas=0` would measurably slow Cassandra's native transport. Neither side's netty
  tuning may be adopted blindly; see `docs/spikes/opensearch-embedding.md`.
- Cassandra *writes* `io.netty.transport.estimateSizeOnSubmit` at runtime.
- `MaxDirectMemorySize` is one budget that each netty tracks independently, so the two can
  collectively over-commit. Size for the sum.
- Cassandra reads several **unnamespaced** properties — `ssl.enable`, `default.*`,
  `compaction_rate_limit_granularity_in_kb`, `check_data_resurrection_heartbeat_period_milli`. A
  plain `-Dssl.enable=true` set for any other purpose silently turns on Cassandra's JMX SSL client
  mode. It also reads `log4j2.disable.jmx` and `log4j.shutdownHookEnabled`, which OpenSearch's
  log4j2 *uses*.

**Logging is cleanly separated:** logback and slf4j are child-only, so
`LoggingSupportFactory`'s logback sniff resolves per node, and log4j2's MBeans live in the platform
server while ours do not.

**`Thread.setDefaultUncaughtExceptionHandler`** is set process-wide by `CassandraDaemon.setup()` to
`JVMStabilityInspector::uncaughtException`, so it will also receive OpenSearch's uncaught
exceptions. A delegating handler is needed.

**`ThreadAwareSecurityManager.install()`** calls `System.setSecurityManager` process-wide. It needs
`-Djava.security.manager=allow` on 21 and **is removed entirely in JDK 24** — the biggest
forward-looking risk in the stack. It exists only for UDF sandboxing, so
`user_defined_functions_enabled: false` (the default) plus a fork patch to skip `install()` is the
way out.

## Classpath assembly

Use the **Maven dependency tree** (115 jars), which is a strict superset of the fork's `lib/*.jar`
(108). One gap to know about: `cassandra.in.sh` also globs `lib/$(uname -m)/*.jar`, which holds
`AmazonCorrettoCryptoProvider` — absent from the Maven tree and from the local m2. It only warns
because the shipped config sets `fail_on_missing_provider: "false"`; flipping that would stop the
node booting.

## Work items this creates for the fork

1. Add a JDK 18–21 branch to `bin/cassandra.in.sh`.
2. Add `conf/jvm21-server.options`.
3. Make `GCInspector` build `gcStates` from the same MBeanServer it registers its listener on.
4. Make `ThreadAwareSecurityManager.install()` skippable — a JDK 24 blocker.
5. Give `Nodes` a `shutdown()` so its `nodes-info-persistence` thread can be stopped.
6. Namespace the `default.*` / `ssl.enable` / `compaction_rate_limit_*` properties under `cassandra.`.
