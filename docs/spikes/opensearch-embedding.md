# Spike: embedding OpenSearch 3.7.0 in-process

Findings from the de-risking spike, kept because most of them are non-obvious and expensive to
rediscover. Working code is in the session scratchpad under `spike-opensearch/`.

**Verdict: it works**, both on a flat classpath and inside a child `URLClassLoader` — the latter
only if the thread context ClassLoader is pinned. Cold start ≈ 3.0 s, warm restart ≈ 0.2 s. No
SecurityManager, no java agent, no bootstrap checks, no files required on disk.

## The one that matters: pin the TCCL

With OpenSearch in a child loader but the TCCL left pointing at the application loader, the node
**starts, indexes and searches perfectly well — and silently corrupts part of its REST API**:

| REST call | flat | child, TCCL=app | child, TCCL=child |
|---|---|---|---|
| `GET /` | 200 | 200 | 200 |
| `GET /_cluster/health?timeout=30s` | 200 | **400** | 200 |
| `PUT /index`, `PUT /_doc/1`, `POST /_search` | 200 | 200 | 200 |
| `GET /_nodes/stats`, `GET /_cluster/stats` | 200 | **400** | 200 |

```
{"error":{"reason":"no raw transformer found for class org.opensearch.common.unit.TimeValue"},"status":400}
```

The cause is `ServiceLoader.load(Class)` — the one-arg overload, which resolves against the TCCL
— at `XContentBuilder.java:136` and `ErrorOnUnknown.java:75`. It finds zero providers, so
`XContentBuilder.WRITERS` (a `static final`, so the damage is permanent for the life of the
loader) never gets the writers for `TimeValue`, `BytesReference`, `ZonedDateTime`, `Instant`,
`Duration` and friends.

This is exactly the failure mode a smoke test misses: the node reports healthy, CRUD works, and
only some endpoints break. `IsolatedService` therefore brackets *every* SPI call with a TCCL
swap, not just `start()`. OpenSearch builds its thread pools during `Node.<init>`, and threads
inherit their creator's TCCL, so pinning across construct-and-start propagates to every worker.

## Constructing the Node

`Node`'s usable constructor is `protected` and takes `Collection<PluginInfo>` — not
`Collection<Class<? extends Plugin>>` as in older versions — so it must be subclassed, the way
`MockNode` does in the OpenSearch test framework:

```java
static final class EmbeddedNode extends Node {
    EmbeddedNode(Environment env, Collection<PluginInfo> plugins, boolean forbidPrivateIndexSettings) {
        super(env, plugins, forbidPrivateIndexSettings);
    }
}
```

The HTTP/REST layer lives in the `transport-netty4` **module**, which is not auto-discovered from
the classpath. It must be passed explicitly as a classpath plugin, wrapped in `PluginInfo`:

```java
new PluginInfo(clazz.getName(), "classpath plugin", "NA", Version.CURRENT, "21",
               clazz.getName(), null, List.of(), false);
```

The plugin class is `org.opensearch.transport.Netty4ModulePlugin`.

**Bootstrap checks are structurally unreachable.** `Node.validateNodeBeforeAcceptingRequests` is
empty in the base class; only `Bootstrap` overrides it to run `BootstrapChecks`. Constructing
`Node` directly means they never fire, regardless of bind address — nothing had to be disabled.

## Dependencies

Three declared GAVs; the rest comes transitively (74 jars, ~90 MB).

```xml
org.opensearch:opensearch:3.7.0
org.opensearch.plugin:transport-netty4-client:3.7.0
org.apache.logging.log4j:log4j-core:2.25.4
```

`log4j-core` is easy to miss: it is `<optional>true</optional>` in the OpenSearch POM and so is
not inherited, but `Node.<init>` needs it and fails at line 1039 with
`NoClassDefFoundError: org/apache/logging/log4j/core/config/Configurator`.

A greedy per-jar removal sweep found 24 of 74 jars droppable for a create/index/search path, but
that list is not safe to act on — geo, aggregations, ingest and snapshots will re-require several
of them. Declare the three and take the closure.

## JVM flags

Strictly required: **none**. A bare `java -cp ...` passed the full happy path. Recommended:

```
--add-modules=jdk.incubator.vector          # else Lucene falls back to scalar
--add-opens=java.base/java.nio=ALL-UNNAMED  # the only add-opens OpenSearch itself lists for 21+
--enable-native-access=ALL-UNNAMED          # silences a JDK 21 warning; a hard error on JDK 24+
```

Do **not** add `-javaagent:opensearch-agent.jar`. OpenSearch 3.x did replace the SecurityManager
with an agent, but it is installed only by `Bootstrap`, which embedding bypasses — and it would
install a JVM-wide policy that constrains Cassandra too.

## System properties

Required: **none** (`opensearch.path.home` is a CLI-only concern; the `path.home` *Setting* is
what is mandatory). Recommended when sharing the JVM:

```
-Dopensearch.set.netty.runtime.available.processors=false
-Djava.locale.providers=SPI,CLDR
-Dlog4j.shutdownHookEnabled=false -Dlog4j2.disable.jmx=true
-Djava.awt.headless=true -Djna.nosys=true
```

**Deliberately not set:** `io.netty.noUnsafe`, `io.netty.noKeySetOptimization`,
`io.netty.recycler.maxCapacityPerThread`, `io.netty.allocator.numDirectArenas`. These are
JVM-global and Cassandra's Netty reads them too; `noUnsafe=true` in particular measurably slows
Cassandra's native transport. OpenSearch's own Netty tuning must not be copied into a shared JVM.

## Co-hosting notes

- **JNA is never loaded.** Every `Native.register`/`loadLibrary` call sits behind `Bootstrap`,
  which embedding skips. Verified: `/proc/self/maps` shows zero `libjnidispatch` mappings after a
  full start/index/search/close cycle. The classic "native library already loaded in another
  classloader" clash with Cassandra's JNA cannot happen.
- **Netty's `availableProcessors` is a write-once global.** Harmless while Netty is duplicated
  inside each child loader (which the platform-parent design guarantees); the
  `opensearch.set.netty.runtime.available.processors=false` escape hatch covers the rest.
- **Do not call `LogConfigurator`.** It is optional, and it sets four global system properties
  including `java.util.logging.manager`, hijacking JUL process-wide and pulling Cassandra's JUL
  output into OpenSearch's log4j2 configuration.
- **Never populate `modules/` or `plugins/` on disk.** `PluginsService` runs a jar-hell check
  against `System.getProperty("java.class.path")` for each bundle found there — which in an
  isolated setup is *Cassandra's* classpath, producing both missed conflicts and spurious
  `IllegalStateException: jar hell!`. Classpath plugins passed as `PluginInfo` skip that path
  entirely.
- **Exclude `netty-codec-native-quic*`** unless HTTP/3 is needed: it ships real JNI natives and
  would become a genuine two-classloader hazard alongside Cassandra's Netty.
- `zstd-jni` also carries JNI natives and is required by `opensearch-compress`; it must not end up
  loaded by two loaders.

## Known cosmetic gap

OpenSearch registers `java.util.spi.CalendarDataProvider` → `IsoCalendarDataProvider` to make
`Locale.ROOT` follow ISO week rules. The JDK loads locale SPI with the **system** classloader,
never the TCCL, so under isolation this provider is not picked up and `Locale.ROOT` reverts to
`firstDayOfWeek=1, minimalDays=1`. It affects only `weekyear`/`weekyear_week_day` formats at year
boundaries; most date paths use a hardcoded `MONDAY,4`. A 900-byte shim jar on the system
classpath fixes it if needed.

## Shutdown

`node.close()` + `awaitClose(30s)` returns true and releases everything. Exactly one non-daemon
thread outlives it — Netty's `globalEventExecutor-1-1` — which self-terminates after ~1 s idle
and does not block JVM exit. No shutdown hooks, no `System.exit`, and no
`setDefaultUncaughtExceptionHandler` (all of those live in `Bootstrap`). Three full
construct/start/close cycles in one JVM released and rebound the ports each time, with thread
count returning to baseline.
