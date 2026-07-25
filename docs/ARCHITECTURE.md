# Architecture

## What this is

A single JVM process that hosts a full Apache Cassandra node **and** a full OpenSearch node,
each loaded in its own isolated ClassLoader, under a supervisor that gives them one shared
lifecycle. Both servers are real — not embedded test doubles — so the stock operational tools
(`nodetool`, `cqlsh`, the OpenSearch REST API) work against them unchanged.

## Why ClassLoader isolation

Cassandra and OpenSearch cannot share a flat classpath. They disagree on Netty, Guava, Jackson,
Lucene, Log4j and Airline versions, and each one calls `System.setProperty`, installs static
singletons and registers MBeans under the assumption that it owns the JVM. Merging their
classpaths produces a build that either fails to resolve or fails at runtime in ways that shift
with every dependency bump.

The technique used here is the one Cassandra's own `dtest` framework uses to run several
Cassandra nodes in one JVM: a **parent-last** `URLClassLoader` per service whose delegation is
inverted for everything except a tiny shared surface. See
`test/distributed/org/apache/cassandra/distributed/shared/InstanceClassLoader.java` in the
Cassandra tree for the original. We reproduce the approach; we do **not** depend on the dtest
libraries, which are test artifacts and shade their dependencies.

```
                    ┌─────────────────────────────────────────┐
                    │            System ClassLoader           │
                    │  supervisor, SPI, snakeyaml, slf4j-api  │
                    └────────────────┬────────────────────────┘
                     Each child's *parent* is the platform
                     loader (JDK only). The application
                     classpath is reached for exactly one
                     package: io.cassandraopensearch.spi.*
              ┌──────────────┴──────────────┐
              ▼                             ▼
  ┌───────────────────────┐     ┌────────────────────────┐
  │ IsolatedClassLoader   │     │ IsolatedClassLoader    │
  │      "cassandra"      │     │      "opensearch"      │
  │  lib/cassandra/*.jar  │     │  lib/opensearch/*.jar  │
  │                       │     │                        │
  │ CassandraServiceImpl  │     │ OpenSearchServiceImpl  │
  │   → CassandraDaemon   │     │   → org.opensearch     │
  │   → StorageService    │     │       .node.Node       │
  └───────────────────────┘     └────────────────────────┘
```

Everything crossing the boundary goes through `cassandra-opensearch-spi`, which has **zero
dependencies** by design. If the SPI gained a dependency, that dependency would be loaded by the
parent loader and forced on both isolated worlds — exactly the coupling the isolation exists to
prevent. The `spi-must-stay-dependency-free` enforcer rule in `spi/pom.xml` fails the build if
anyone adds one.

Note that `org.slf4j` is deliberately **not** shared. Cassandra binds slf4j to logback and
OpenSearch binds it to log4j2; sharing the API would force a single binding on both and collapse
their logging into one confused configuration. Each loader carries its own API and binding.

### What isolation does *not* solve

ClassLoaders partition *classes*, not the rest of the JVM. These remain global and are handled
explicitly rather than hopefully:

| Global resource | Risk | Handling |
|---|---|---|
| System properties | Both servers read and write them | Namespaces are disjoint in practice (`cassandra.*` vs `opensearch.*`); the audited exceptions are listed in `docs/GLOBAL-STATE.md` |
| Shutdown hooks | Either server calling `System.exit`, or registering a hook, affects the whole process | Both runtimes are forbidden from calling `System.exit`; the supervisor owns the single shutdown hook |
| Native libraries | JNA / Netty native transports load once per JVM per loader | Documented per service; verified by the spikes |
| JMX platform MBean server | One per JVM; both servers register MBeans | Cassandra registers into a **private** MBeanServer that federates reads with the platform one, so nothing of its own lands in the platform namespace. The supervisor's `io.cassandraopensearch:type=Supervisor` goes on the platform server, reached through the launcher's standard `com.sun.management.jmxremote.*` agent — **not** through a connector the supervisor starts itself. Cassandra's own port (7199) will not reach the supervisor MBean, because its federated server routes every non-JDK domain to the private server. |
| Ports | Obvious collision | All ports are supervisor-assigned in `cassandra-opensearch.yaml` |
| Logging | Cassandra uses **logback**, OpenSearch uses **log4j2** | Different frameworks in different loaders, so no `LogManager` fight; each writes its own file under `logs/` |

## Modules

| Module | Loaded by | Purpose |
|---|---|---|
| `spi` | parent | The bridge contract. Zero dependencies. |
| `bootstrap` | parent | `IsolatedClassLoader`, classpath assembly, handle that proxies SPI calls into a child loader with the right TCCL. |
| `runtime-cassandra` | **cassandra child** | `EmbeddedService` over `CassandraDaemon`. Compiles against Cassandra; never on the supervisor's classpath. |
| `runtime-opensearch` | **opensearch child** | `EmbeddedService` over `org.opensearch.node.Node`. Compiles against OpenSearch; never on the supervisor's classpath. |
| `server` | parent | Entry point, configuration model, supervision, decommission coordination, supervisor MBean. |
| `cli` | parent | `bin/*` wrappers, including `nodetool` passthrough. |
| `dist` | — | Assembles the tarball and the Docker image. |
| `integration-tests` | — | Failsafe ITs that drive the assembled tarball as a real process. |

The two `runtime-*` modules are the reason this works: they are compiled against their server's
API at build time but are placed in `lib/cassandra/` and `lib/opensearch/` at assembly time, so
they are only ever loaded by the corresponding child loader.

## Lifecycle

Startup is ordered, because Cassandra owns the node identity that OpenSearch's node name and
data paths derive from:

1. Supervisor reads `conf/cassandra-opensearch.yaml`, resolves directories and ports.
2. Cassandra child loader created → `start()` → blocks until the node is in the ring and CQL is
   accepting connections.
3. OpenSearch child loader created → `start()` → blocks until the node has joined and cluster
   health is at least YELLOW.
4. Supervisor registers its MBean and reports `RUNNING`.

Shutdown reverses it: OpenSearch first (it is the dependent service), then Cassandra, then the
JVM exits. The supervisor owns the only shutdown hook.

## Decommission

This is the one genuinely coupled operation, and the ordering is not arbitrary. An OpenSearch
node that still holds primary shards when the process exits loses those shards; therefore
OpenSearch must relocate its data *before* Cassandra tears the node out of the ring, since once
Cassandra has left, the process is on its way out.

```
  operator: bin/cassandra-opensearch decommission
        │
        ├─1─ prepareDecommission(opensearch)   → cluster.routing.allocation.exclude._name=<node>
        ├─2─ prepareDecommission(cassandra)    → mark leaving, stop accepting new ownership
        │
        ├─3─ awaitDecommissionReady(opensearch) → poll until 0 shards remain on this node
        │                                          (progress reported to the CLI)
        ├─4─ decommission(cassandra)           → StorageService.decommission(), streams ranges
        │
        ├─5─ stop(opensearch)  → node.close()
        ├─6─ stop(cassandra)   → drain + shutdown
        └─7─ exit 0
```

`--force` proceeds past step 3 with shards outstanding; `--timeout` bounds each waiting phase.

Anything that fails, is refused or is cancelled up to and including step 3 is backed out:
`abortDecommission` is called on every service already prepared, in reverse order, and OpenSearch
takes this node back out of `cluster.routing.allocation.exclude._name`. This is not a nicety.
Cassandra refuses a single-node decommission in step 2, *after* step 1 has excluded the node, and
an exclusion nobody removes means the cluster never allocates a shard here again — one mistyped
command would leave every index created afterwards UNASSIGNED. From step 4 on there is nothing to
back out: the ranges are already streaming.

The coordinator runs one more step than the four above: `decommission` is called on OpenSearch as
well, before Cassandra's. That is where the shard count is re-checked, `--force` is enforced and
recorded, and the node moves to `DECOMMISSIONED` rather than merely `STOPPED`. It runs first
because Cassandra's is the irreversible one.

### The unsupervised path is watched, not made safe

A plain `nodetool decommission` bypasses the supervisor and talks straight to Cassandra's
`StorageService` MBean. That path works — it is Cassandra's own — but it does **not** order
OpenSearch's relocation first, so shards resident on this node can be lost when the process exits.

The supervisor now watches for it. The Cassandra runtime runs a `RingStateWatcher`: a daemon
thread that polls `StorageService.getOperationMode()` once a second and reports
`ring.state.changed` whenever the mode moves, saying which mode and whether it means this node is
leaving the ring. Polling rather than a JMX `NotificationListener` because there is nothing to
listen to — `StorageService` extends `NotificationBroadcasterSupport`, but `setMode`, the one
method every transition in the class goes through, writes a field and logs, and the only
notifications that class emits come from its `JMXProgressSupport`, wired to bootstrap-resume and
repair.

```
  operator: bin/nodetool decommission
        │
        ├── StorageService.decommission()  → mode LEAVING, then sleeps RING_DELAY (30 s)
        │
   RingStateWatcher (1 s poll)
        └─→ ring.state.changed  state=LEAVING leaving=true
              │
        Supervisor.onNodeLeavingTheRing
              ├── watch_external_decommission false?          → nothing
              ├── DECOMMISSIONING, or a coordinator in flight? → nothing; this is ours
              └── otherwise → WARN, then prepareDecommission(opensearch) on its own thread
```

Two things about that handler are load-bearing. It **must not fire during the supported path**,
which drives Cassandra into the identical modes; it tells them apart by the supervisor's own
state, not by inspecting the event, because a coordinated decommission is always
`DECOMMISSIONING` by the time Cassandra begins to leave. And it **hands the work to another
thread**: `reportEvent` is called from inside the service, frequently on a Cassandra thread, and
the SPI says it never blocks.

The message grammar is `io.cassandraopensearch.spi.RingStateEvent`, in the SPI rather than agreed
by convention between two modules that share no other code. The service reports the verdict
(`leaving=true`) as well as the mode, so the supervisor does not have to carry a table of
Cassandra's operation-mode names — that vocabulary belongs to the runtime that owns it.

**This reduces harm; it does not make the path safe.** By the time Cassandra reports `LEAVING` the
ring transition has already started, so the exclusion is a catch-up racing it, and the supervisor
has no way to hold anything up while the shards move — `nodetool` is not waiting for it and
nothing stops the operator killing the process. It also stops at the exclusion: it does not wait
for relocation and then stop OpenSearch, because that wait would keep a second driver on the
service's lifecycle for up to `shard_relocation_timeout`. `bin/cassandra-opensearch decommission`
is still the only ordering-safe way to retire a node. See `docs/KNOWN-GAPS.md` §1 for the
remaining exposure and how to work around it.

## Distribution layout

```
cassandra-opensearch-1.0.0/
├── bin/
│   ├── cassandra-opensearch     # start | stop | status | decommission
│   ├── nodetool                 # passthrough to the embedded Cassandra's JMX
│   ├── cqlsh                    # passthrough
│   └── opensearch-plugin, opensearch-keystore
├── conf/
│   ├── cassandra-opensearch.yaml   # supervisor: ports, dirs, timeouts, coupling policy
│   ├── cassandra.yaml              # stock Cassandra config
│   ├── opensearch.yml              # stock OpenSearch config
│   ├── jvm21-server.options        # synthesized; see docs/JDK.md
│   ├── logback.xml                 # Cassandra's logging
│   └── log4j2.properties           # OpenSearch's logging
├── lib/
│   ├── boot/          # supervisor + SPI + bootstrap  → system classpath
│   ├── cassandra/     # Cassandra + runtime-cassandra → cassandra child loader
│   └── opensearch/    # OpenSearch + runtime-opensearch → opensearch child loader
├── data/  logs/
```

Only `lib/boot/` is on the JVM's `-cp`. The other two directories are read by the bootstrap
module and turned into child loaders.
