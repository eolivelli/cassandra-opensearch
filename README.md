# cassandra-opensearch

Runs a full **Apache Cassandra** node and a full **OpenSearch** node **inside a single JVM**,
each in its own isolated ClassLoader, under one supervisor that gives them a shared lifecycle.

Both are real servers, not embedded test harnesses. The stock operational tooling works against
them unchanged: `nodetool`, `cqlsh`, and the OpenSearch REST API all talk to the running process
exactly as they would to a standalone install.

```
┌──────────────────────── one JVM, one process ────────────────────────┐
│                                                                      │
│   supervisor  ──────────────┬──────────────────────────┐             │
│   (lifecycle, decommission) │                          │             │
│                             ▼                          ▼             │
│              ┌──────────────────────┐   ┌──────────────────────┐     │
│              │  Cassandra 5.0.7     │   │  OpenSearch 3.7.0    │     │
│              │  isolated loader     │   │  isolated loader     │     │
│              │  CQL :9042           │   │  REST :9200          │     │
│              │  JMX :7199           │   │  transport :9300     │     │
│              └──────────────────────┘   └──────────────────────┘     │
└──────────────────────────────────────────────────────────────────────┘
      ▲                                          ▲
   nodetool, cqlsh                        curl, any OpenSearch client
```

---

## Table of contents

- [The problem, and the design](#the-problem-and-the-design)
- [Requirements](#requirements)
- [Build](#build)
- [Run a single node](#run-a-single-node)
- [Run a multi-node cluster on one machine](#run-a-multi-node-cluster-on-one-machine)
- [Decommissioning a node](#decommissioning-a-node)
- [Configuration](#configuration)
- [Docker](#docker)
- [Testing](#testing)
- [Project layout](#project-layout)
- [Documentation](#documentation)

---

## The problem, and the design

### Why they cannot simply share a classpath

Cassandra and OpenSearch disagree, concretely and irreconcilably, on Netty (4.1 vs 4.2), Lucene
(9.8 vs 10.4), Guava, Jackson (two major lines), Log4j and zstd-jni. Beyond version skew, each one
calls `System.setProperty`, installs static singletons, registers MBeans and hooks
`Thread.setDefaultUncaughtExceptionHandler` on the assumption that it owns the JVM.

Merging the two classpaths yields a build that either fails to resolve or fails at runtime in ways
that shift with every dependency bump. Letting Maven mediate a single version for both is worse:
it produces something that starts and then misbehaves subtly.

### ClassLoader isolation

Each service gets a **parent-last `URLClassLoader`** over its own jars. This is the technique
Cassandra's own `dtest` framework uses to run several Cassandra nodes in one JVM — we reproduce the
approach but do not depend on the dtest artifacts, which are test jars with shaded dependencies.

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
  │  (117 jars)           │     │  (69 jars)             │
  │                       │     │                        │
  │ CassandraService      │     │ OpenSearchService      │
  │   → CassandraDaemon   │     │   → org.opensearch     │
  │   → StorageService    │     │       .node.Node       │
  └───────────────────────┘     └────────────────────────┘
```

The key design decision is that **each child's parent is the *platform* ClassLoader, not the
application one**. Platform resolves the JDK and knows nothing of the application classpath, so
ordinary parent-first delegation already keeps the supervisor's classes out. Only
`io.cassandraopensearch.spi.*` is routed outward explicitly.

This matters more than it looks. `dtest` decides what to share by **package prefix** — `javax.`,
`com.sun.`, `sun.` — which is safe there only because dtest's parent loader holds the entire
Cassandra classpath, so a prefix miss silently resolves anyway. Ours deliberately holds none of it,
and `javax.annotation.Nullable` is **jsr305, not JDK**. Prefix matching sends it to the wrong
loader and the node dies during config parsing. Delegating to the platform loader answers the
question "is this a JDK class?" correctly by construction, with no name list to get wrong.

`org.slf4j` is deliberately **not** shared either: Cassandra binds it to logback and OpenSearch to
log4j2, so sharing the API would force one binding on both and collapse their logging into a single
confused configuration. Each loader carries its own API and binding.

### The bridge

Everything crossing the boundary goes through `cassandra-opensearch-spi`, which has **zero
dependencies** — enforced by a build rule, not convention. Any dependency added there would be
loaded by the parent and forced on both isolated worlds, which is precisely the coupling the
isolation exists to prevent.

```java
public interface EmbeddedService {
    String name();
    void start(ServiceContext context) throws Exception;
    ServiceStatus status();
    Map<String, String> details();

    void prepareDecommission(DecommissionContext context) throws Exception;
    boolean awaitDecommissionReady(DecommissionContext context) throws Exception;
    void abortDecommission(DecommissionContext context) throws Exception;
    void decommission(DecommissionContext context) throws Exception;
    void stop() throws Exception;
}
```

Every type in these signatures is a JDK type or an SPI type. Returning, say, a Cassandra
`TokenMetadata` would throw `NoClassDefFoundError` in the supervisor.

Every call is bracketed by a **thread context ClassLoader swap**. This is not defensive
housekeeping — it is load-bearing. With OpenSearch in a child loader and the TCCL left pointing at
the application loader, the node starts, reports healthy, and indexes and searches perfectly well,
while `/_cluster/health`, `/_nodes/stats` and `/_cluster/stats` **silently return HTTP 400**. The
cause is `ServiceLoader.load(Class)` resolving against the TCCL and leaving
`XContentBuilder.WRITERS` — a `static final` — permanently missing its type writers. An integration
test asserts those three endpoints return 200.

### What isolation does *not* solve

ClassLoaders partition *classes*, not the JVM. These stay global and are handled explicitly:

| Global resource | Handling |
|---|---|
| `System.exit` | Both of Cassandra's exit paths are neutralised — `CassandraDaemon(runManaged=true)` **and** `JVMStabilityInspector.replaceKiller`, which otherwise calls `System.exit(100)` on OOM or disk failure |
| Shutdown hooks | Cassandra's JVM-global drain hook is removed; the supervisor owns the process's only hook |
| Uncaught exception handler | Cassandra sets one process-wide; a delegating handler preserves whatever was installed before |
| Native libraries | Verified safe: JNA, netty-epoll and tcnative each extract to a per-loader temp file, and the JVM's "already loaded" rule is per *file* |
| `io.netty.*` properties | JVM-global, read by both Netty copies, and genuinely unreconcilable. `conf/jvm21-server.options` ships **three, from Cassandra's side**: `tryReflectionSetAccessible=true`, `allocator.useCacheForAllThreads=true`, `allocator.maxOrder=11`. OpenSearch's Netty gets them too — `maxOrder=11` quadruples its arena chunk to 16 MiB. OpenSearch's own preferences (`noUnsafe=true`, `numDirectArenas=0`) are **not** adopted, because they measurably slow Cassandra's native transport. See [docs/KNOWN-GAPS.md](docs/KNOWN-GAPS.md) §6 |
| JMX | Cassandra registers into a *private* MBeanServer that federates reads with the platform one, so nothing of its lands in the platform namespace |
| Logging | logback and log4j2, in different loaders, each writing its own file under `logs/` |

### Lifecycle

Startup is ordered, because OpenSearch's node identity derives from the Cassandra node:

1. Supervisor reads `conf/cassandra-opensearch.yaml`.
2. Cassandra loader → `start()` → blocks until the node is `NORMAL` and CQL is accepting.
3. OpenSearch loader → `start()` → blocks until it has joined and health is at least YELLOW. Its
   `node.name` is the Cassandra host id.
4. Supervisor registers its MBean and reports `RUNNING`.

Shutdown reverses it — OpenSearch first, then Cassandra — then the JVM exits.

---

## Requirements

- **JDK 21.** Not optional, and not 17.
- Maven 3.9+
- Linux or macOS (multi-node on one machine needs loopback aliases; see below)

> ### ⚠️ You probably cannot build this as-is
>
> The Cassandra half depends on **`com.datastax.dse:dse-db-all:5.0.7.0-SNAPSHOT`**, which is not
> published to Maven Central. It is built from a private fork and installed into the local
> repository (see [Build](#build)). Without it, `runtime-cassandra`, `dist` and the integration
> tests will not resolve.
>
> Everything else does build from public artifacts: the SPI, the ClassLoader isolation, the
> supervisor, the CLI, and the entire OpenSearch runtime including tests that start a real node.
> That is what CI covers — see [`.github/workflows/build.yml`](.github/workflows/build.yml).
>
> Substituting `org.apache.cassandra:cassandra-all` for the fork is untried. The two spike
> documents record several places where this code depends on **fork-specific** behaviour —
> `org.apache.cassandra.nodes.Nodes`, which does not exist upstream, and a `StorageService`
> that completes a decommission and then throws — so expect that swap to be work, not a
> one-line change.

### Why JDK 21 and not 17

OpenSearch 3.x jars are compiled to **Java 21 bytecode**, verified directly rather than taken from
documentation:

```
$ unzip -p opensearch-3.7.0.jar org/.../ClusterStatsNodes\$JvmVersion.class | xxd | head -1
00000000: cafe babe 0000 0041 ...
                       ^^^^ 0x41 = 65 = Java 21
```

A JDK 17 JVM rejects class file major 65 with `UnsupportedClassVersionError` at load time. No
compiler flag or classloader trick changes this — it is the runtime refusing to parse the file.
Since both servers must share one JVM, that JVM is bounded below by OpenSearch's 21.

Cassandra is consumed as **class-61 (Java 17) jars**, which load fine on 21. The fork's `build.xml`
hard-fails on any JDK outside `java.supported = "11,17,22"`, so it is built with 17 and installed
to the local repository. `maven-enforcer-plugin` fails this project's build on anything but 21.

Full reasoning in [docs/JDK.md](docs/JDK.md).

---

## Build

```bash
export JAVA_HOME=~/.sdkman/candidates/java/21.0.11-tem

# One-time: build the Cassandra fork and install it to ~/.m2
cd ~/dev/cassandra && JAVA_HOME=~/.sdkman/candidates/java/17.0.19-tem ant mvn-install

cd ~/dev/cassandra-opensearch
mvn clean install          # unit tests            (~70 s)
mvn clean verify           # + integration tests   (~10.5 min)
mvn verify -Pdocker        # + builds the Docker image
```

The tarball lands in `dist/tarball/target/cassandra-opensearch-1.0.0-SNAPSHOT-bin.tar.gz` (113 MB).

---

## Run a single node

```bash
tar xzf dist/tarball/target/cassandra-opensearch-*-bin.tar.gz
cd cassandra-opensearch-1.0.0-SNAPSHOT

bin/cassandra-opensearch start        # foreground (also writes the pid file, so `stop`
                                      #  works from another terminal)
bin/cassandra-opensearch start -d     # background
```

Then:

```bash
bin/cassandra-opensearch status
bin/nodetool status
bin/nodetool info
curl 'localhost:9200/_cluster/health?pretty'
curl -XPUT 'localhost:9200/my-index/_doc/1' -H 'Content-Type: application/json' -d '{"hello":"world"}'
```

```bash
bin/cassandra-opensearch stop
```

`start` refuses outright if something is already answering on the supervisor's JMX endpoint, rather
than launching a second JVM that dies on a port conflict while the readiness probe cheerfully
reports the *first* node as "up".

> **Ports.** The defaults are the stock ones (9042, 9200, 7199, 7000). If you already have
> Cassandra or OpenSearch running — including in a container published on `0.0.0.0`, which takes
> *every* loopback alias with it — change them in `conf/cassandra-opensearch.yaml` **and**
> `conf/cassandra.yaml`. Both files, because Cassandra parses its own; see
> [docs/KNOWN-GAPS.md](docs/KNOWN-GAPS.md) §2.

---

## Run a multi-node cluster on one machine

A node is a whole JVM hosting both servers, so several nodes means **several processes**, exactly
as across several machines. `examples/local-cluster/` automates it.

```bash
cd examples/local-cluster

./create-cluster.sh -n 3 -d /tmp/my-cluster
./cluster-control.sh -d /tmp/my-cluster start
```

```
node1  127.0.0.1  cql 21012  rest 21015  nodetool-jmx 21013  supervisor-jmx 21014
node2  127.0.0.2  cql 21022  rest 21025  nodetool-jmx 21023  supervisor-jmx 21024
node3  127.0.0.3  cql 21032  rest 21035  nodetool-jmx 21033  supervisor-jmx 21034
```

Verified output from a real 2-node run:

```bash
$ ./cluster-control.sh -d /tmp/my-cluster ring
Datacenter: datacenter1
=======================
--  Address    Load        Tokens  Owns (effective)  Host ID                               Rack
UN  127.0.0.1  118.64 KiB  16      100.0%            d36f3a06-1dba-46c8-a389-9030408afaff  rack1
UN  127.0.0.2  121.29 KiB  16      100.0%            7b29362e-00ee-4555-99b1-f1ab9cbf3827  rack1

$ ./cluster-control.sh -d /tmp/my-cluster health
ip        heap.percent cpu node.roles                                        cluster_manager name
127.0.0.1           18   6 cluster_manager,data,ingest,remote_cluster_client *               d36f3a06-...
127.0.0.2           13   6 cluster_manager,data,ingest,remote_cluster_client -               7b29362e-...
```

Note the OpenSearch node names **are** the Cassandra host ids — that is the coupled identity, and
it is what the decommission shard-exclusion keys on.

Other commands:

```bash
./cluster-control.sh -d /tmp/my-cluster status         # supervisor status, every node
./cluster-control.sh -d /tmp/my-cluster stop           # reverse order
./cluster-control.sh -d /tmp/my-cluster decommission 2 # retire node 2, coupled
./cluster-control.sh -d /tmp/my-cluster destroy        # stop and delete (refuses if a
                                                       #  node would not stop; -f overrides)
```

### What the script has to get right

Worth understanding even if you use the script, because these are the things that fail confusingly:

- **Node `i` binds `127.0.0.i`.** Linux routes all of `127.0.0.0/8` to `lo` with no configuration.
  **macOS does not** — you must add each alias first:
  ```bash
  sudo ifconfig lo0 alias 127.0.0.2 up
  sudo ifconfig lo0 alias 127.0.0.3 up
  ```
- **Ports differ per node too**, not just addresses. Differing addresses alone would satisfy
  Cassandra — that is what `ccm` does — but the JDK management agent, Cassandra's JMX connector and
  OpenSearch's transport each bind on their own terms, and one of them choosing the wildcard
  address turns a shared port into a collision that presents as "a node failed to start".
- **Both config files must be edited and must agree.** Cassandra reads `conf/cassandra.yaml`
  itself; the supervisor reads `conf/cassandra-opensearch.yaml`.
- **A second node needs `jmx_address`**, which the shipped file omits because `127.0.0.1` is the
  default. Without it, two nodes fight over one JMX address:port.
- **OpenSearch's `discovery.type: single-node` must be replaced** by `discovery.seed_hosts` and
  `cluster.initial_cluster_manager_nodes` — a single-node-discovery node elects itself and never
  forms a cluster with anyone. The bootstrap list must use **transport addresses, not node names**,
  because a node's name is its Cassandra host id, unknowable before it boots.
- **Start order matters.** Cassandra reads `seed_provider` at daemon initialisation and OpenSearch
  resolves its discovery settings while `Node` is being constructed. A node that cannot reach node 1
  within its discovery timeout will bootstrap a *second* cluster of its own.

See [examples/README.md](examples/README.md) for doing this by hand.

---

## Decommissioning a node

```bash
bin/cassandra-opensearch decommission [--timeout 30m] [--force]
```

This is the **only ordering-safe path**, and the ordering is the entire point. An OpenSearch node
still holding primary shards when the process exits loses them, so OpenSearch must relocate its
data *before* Cassandra tears the node out of the ring — because once Cassandra has left, the
process is on its way out.

```
  ├─1─ prepareDecommission(opensearch)    → cluster.routing.allocation.exclude._name=<node>
  ├─2─ prepareDecommission(cassandra)     → may refuse here (e.g. sole ring member)
  ├─3─ awaitDecommissionReady(opensearch) → poll until 0 shards remain, reporting progress
  ├─4─ decommission(opensearch)           → re-check shard count, enforce --force
  ├─5─ decommission(cassandra)            → StorageService.decommission(), streams ranges
  ├─6─ stop(opensearch) → stop(cassandra)
  └─7─ exit 0
```

Anything that fails, is refused, is cancelled or times out **up to and including step 3** is backed
out: `abortDecommission` runs on every prepared service in reverse, removing this node from the
OpenSearch allocation exclusion. Once step 5 begins there is no way back, and none is attempted.

Real output from the integration suite:

```
[prepare-opensearch]      excluded 29bb171c-… from shard allocation
[await-shard-relocation]  relocating 2 of 2 shards
[await-shard-relocation]  relocated all 2 shards
[decommission-cassandra]  streaming ranges to the rest of the ring
[decommission-cassandra]  left the ring
decommission complete: OpenSearch relocated its shards; Cassandra streamed its ranges and
                       left the ring; both services stopped
Shutdown complete; exit code 0
```

**`--force` is required below four nodes.** `system_distributed` and `system_auth` are RF 3, so
Cassandra refuses an unforced decommission that would take the ring under the replication factor.

> ⚠️ **`nodetool decommission` is not equivalent.** It talks straight to Cassandra's
> `StorageService` and bypasses the supervisor. With `watch_external_decommission` on, the
> supervisor notices and excludes this node from shard allocation so the shards at least start
> moving — but that is a catch-up racing a ring transition that has already begun, not the
> ordering guarantee above. Let `_cat/shards` empty before stopping the process. See
> [docs/KNOWN-GAPS.md](docs/KNOWN-GAPS.md) §1.

---

## Configuration

| File | Owns |
|---|---|
| `conf/cassandra-opensearch.yaml` | The supervisor: ports, directories, timeouts, decommission policy |
| `conf/cassandra.yaml` | Cassandra, stock format, parsed by Cassandra |
| `conf/opensearch.yml` | OpenSearch, stock format |
| `conf/jvm21-server.options` | JVM flags for the whole process |
| `conf/logback.xml` / `conf/log4j2.properties` | Cassandra's and OpenSearch's logging |

The supervisor file deliberately does **not** duplicate the servers' own settings, so existing
operational knowledge and tooling keep working. Unknown keys and unit-less durations are **rejected
at startup** rather than ignored — a silently-dropped setting leaves an operator believing a limit
is in force when it is not.

```yaml
cluster_name: cassandra-opensearch

services:
  cassandra:
    jmx_port: 7199          # bin/nodetool connects here
    jmx_address: 127.0.0.1  # add this on any node not on 127.0.0.1
    startup_timeout: 5m
  opensearch:
    http_port: 9200
    transport_port: 9300
    network_host: 127.0.0.1
    startup_timeout: 5m

decommission:
  shard_relocation_timeout: 30m
  ring_streaming_timeout: 1h

supervisor:
  health_check_interval: 10s
```

Environment variables read by the launcher: `JAVA_HOME`, `MAX_HEAP_SIZE`,
`MAX_DIRECT_MEMORY_SIZE`, `JVM_EXTRA_OPTS`, `CASSANDRA_OPENSEARCH_JMX_HOST`,
`CASSANDRA_OPENSEARCH_JMX_PORT`.

---

## Docker

```bash
mvn verify -Pdocker
docker run --rm -it cassandra-opensearch:1.0.0-SNAPSHOT
```

446 MB, on `eclipse-temurin:21-jre`, non-root, with a `HEALTHCHECK` that waits for both services.

Use **`docker stop`**, not `docker exec ... stop`: the latter does reach SIGTERM, but the exec is
killed along with PID 1, so its exit code tells you nothing. The ordered shutdown takes about 8
seconds on an idle single node, uncomfortably close to Docker's 10-second SIGKILL default — raise
`--stop-timeout` if the node holds real data.

> The shipped config binds **loopback only**, so published ports are not reachable without mounting
> a modified `conf/`. Cassandra additionally needs `broadcast_address` and `broadcast_rpc_address`
> for a `0.0.0.0` bind.

---

## Testing

```bash
mvn test      # 229 unit tests
mvn verify    # + 22 integration tests   (~10.5 min total)
```

Integration tests unpack the tarball and drive it as a **real external process** — the shipped
artifact, not an embedded harness. They cover the full lifecycle, a CQL round trip, an OpenSearch
REST round trip, the TCCL-corruption canary, `nodetool`, both servers proven to be in one JVM, and
a **two-node coupled decommission** with real shard relocation and real ring streaming.

They use ports 21100–21310 and never the stock ones.

---

## Project layout

| Module | Loaded by | Purpose |
|---|---|---|
| `spi` | parent | The bridge contract. Zero dependencies, enforced. |
| `bootstrap` | parent | `IsolatedClassLoader`, classpath assembly, TCCL-swapping proxy. |
| `runtime-cassandra` | **cassandra child** | `EmbeddedService` over `CassandraDaemon`. |
| `runtime-opensearch` | **opensearch child** | `EmbeddedService` over `org.opensearch.node.Node`. |
| `server` | parent | Entry point, config model, supervision, decommission, MBean. |
| `cli` | parent | `bin/*` wrappers. |
| `dist` | — | Tarball and Docker image (an aggregator; see below). |
| `integration-tests` | — | Failsafe ITs driving the assembled tarball. |
| `examples` | — | Multi-node scripts and worked examples. |

`dist` is an aggregator of three modules rather than one because a single POM **cannot** produce
this layout: Maven mediates versions across the whole graph, so one resolution would hand both
isolated loaders the same mediated Netty and Lucene — quietly undoing the isolation.

---

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — isolation design, module map, lifecycle
- [docs/JDK.md](docs/JDK.md) — the JDK 17 / OpenSearch 3.x conflict
- [docs/KNOWN-GAPS.md](docs/KNOWN-GAPS.md) — **what is not finished or not safe. Read this.**
- [docs/REMAINING-WORK.md](docs/REMAINING-WORK.md) — the prioritised to-do list, and what was found in review
- [docs/spikes/](docs/spikes/) — how each server is embedded, and the evidence behind it
- [examples/README.md](examples/README.md) — multi-node walkthrough
