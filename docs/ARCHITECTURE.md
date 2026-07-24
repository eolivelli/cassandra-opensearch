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
| JMX platform MBean server | One per JVM; both servers register MBeans | Distinct domains (`org.apache.cassandra.*` vs the supervisor's `io.cassandraopensearch:*`); JMX RMI connector is owned by the supervisor |
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

A plain `nodetool decommission` bypasses the supervisor and talks straight to Cassandra's
`StorageService` MBean. That path still works — it is Cassandra's own — but it does **not**
order OpenSearch's relocation first. The Cassandra runtime therefore watches its own operation
mode and reports `ring.state.changed` back to the supervisor, which reacts by excluding the
OpenSearch node as a best-effort catch-up. The supervisor-driven path is the supported one and
is what `bin/cassandra-opensearch decommission` invokes; this is called out in the README.

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
