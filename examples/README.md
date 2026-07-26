# Examples

Worked examples for running `cassandra-opensearch`. This module contains **no code** — only
scripts and documentation — and produces no artifact.

- [`local-cluster/`](local-cluster/) — an N-node cluster on a single machine
- [Doing it by hand](#doing-it-by-hand) — what the scripts actually change, and why
- [Worked example: coupled decommission](#worked-example-coupled-decommission)
- [Troubleshooting](#troubleshooting)

---

## Quick start

```bash
export JAVA_HOME=~/.sdkman/candidates/java/21.0.11-tem
cd examples/local-cluster

./create-cluster.sh -n 3 -d /tmp/my-cluster
./cluster-control.sh -d /tmp/my-cluster start
./cluster-control.sh -d /tmp/my-cluster ring
./cluster-control.sh -d /tmp/my-cluster health
```

`create-cluster.sh` unpacks the distribution once per node and rewrites the configuration. Each
node is a complete, independent installation — a node is a whole JVM hosting both servers, so
"three nodes on one machine" means three processes, exactly as it would across three machines.

| Command | Effect |
|---|---|
| `start [n]` | start every node in order, or just node `n` |
| `stop [n]` | stop every node in reverse order, or just node `n` |
| `status [n]` | supervisor status |
| `ring` | `nodetool status` from node 1 |
| `health` | OpenSearch cluster health and node list |
| `decommission n` | retire node `n` through the coupled procedure |
| `destroy` | stop everything and delete the directory — refuses if a node would not stop, unless `-f` |

A node that was already stopped is not a node that "would not stop": the launcher reports exit
code 3 for it, which is the CLI's "nothing is running", and `destroy` treats that as success. So
destroying a cluster you have just stopped, or one you never started, needs no `-f`. Options may
be written on either side of the command, so `destroy -f` and `-f destroy` are the same thing.

Exit code 3 means the node's recorded process is gone as well as its JMX port being silent. A node
whose JVM is alive but not yet answering — a `start -d` that timed out, one interrupted before the
supervisor finished starting — exits 1 instead, and `destroy` refuses it: deleting the directory
of a live node leaves it writing into files nothing can find.

### Port plan

Node `i` binds `127.0.0.i`, with ports at `21000 + i*10 + offset`:

| Offset | Purpose | Node 1 | Node 2 |
|---|---|---|---|
| 0 | Cassandra storage (gossip, streaming) | 21010 | 21020 |
| 1 | Cassandra SSL storage | 21011 | 21021 |
| 2 | **CQL** | 21012 | 21022 |
| 3 | **Cassandra JMX** (`nodetool`) | 21013 | 21023 |
| 4 | **Supervisor JMX** (`status`, `decommission`) | 21014 | 21024 |
| 5 | **OpenSearch REST** | 21015 | 21025 |
| 6 | OpenSearch transport | 21016 | 21026 |

Nothing uses a stock port. A developer machine very often already has something on 9042 or 9200,
and if that something is a container published on `0.0.0.0`, it takes **every** loopback alias with
it — moving to `127.0.0.2` does not help, and the failure arrives as an opaque "address already in
use" from inside a child ClassLoader.

Note there are **two** JMX endpoints per node, deliberately. Cassandra's federated MBean server
routes every non-JDK domain to its private server, so the supervisor MBean is not reachable on
Cassandra's port. See [../docs/KNOWN-GAPS.md](../docs/KNOWN-GAPS.md) §9.

---

## Doing it by hand

If you would rather not use the scripts, or you are deploying to real machines, this is everything
that has to change for a node that is not the default single node on `127.0.0.1`.

### 1. `conf/cassandra.yaml` — Cassandra parses this itself

```yaml
storage_port: 21020
ssl_storage_port: 21021
native_transport_port: 21022
listen_address: 127.0.0.2
rpc_address: 127.0.0.2

seed_provider:
  - class_name: org.apache.cassandra.locator.SimpleSeedProvider
    parameters:
      - seeds: "127.0.0.1:21010"      # host:port of the seed, for every node including itself
```

### 2. `conf/cassandra-opensearch.yaml` — the supervisor

```yaml
services:
  cassandra:
    listen_address: 127.0.0.2
    storage_port: 21020
    native_transport_port: 21022
    jmx_port: 21023
    jmx_address: 127.0.0.2      # NOT in the shipped file: 127.0.0.1 is its default
  opensearch:
    network_host: 127.0.0.2
    http_port: 21025
    transport_port: 21026
```

`jmx_address` is the one people miss. Without it, every node's Cassandra JMX connector tries to
bind `127.0.0.1` and the second node fails.

> The `listen_address`, `storage_port` and `native_transport_port` keys here **reach nothing** —
> Cassandra takes all three from its own file. They are retained as documentation of what must
> match, which means they must be kept in sync by hand. This is the one place the config model
> knowingly breaks its own rule; see [../docs/KNOWN-GAPS.md](../docs/KNOWN-GAPS.md) §2.

### 3. `conf/opensearch.yml` — replace single-node discovery outright

The distribution ships:

```yaml
discovery.type: single-node
```

A single-node-discovery node **elects itself and never forms a cluster with anyone**. For a real
cluster, delete that line and use:

```yaml
discovery.seed_hosts: ["127.0.0.1:21016", "127.0.0.2:21026", "127.0.0.3:21036"]
cluster.initial_cluster_manager_nodes: ["127.0.0.1:21016"]
network.host: 127.0.0.2
```

Two details that are easy to get wrong:

- **Transport addresses, not node names.** A node's OpenSearch name is the Cassandra host id it is
  assigned at runtime, so nothing knows it before the process has booted. Names cannot be used here.
- **Every node gets the *same* `initial_cluster_manager_nodes`** — one entry, the first node. This
  is what stops a later node from bootstrapping a second cluster of its own when it cannot reach
  node 1 within its discovery timeout.

### 4. Tell the CLI where the supervisor is

```bash
export CASSANDRA_OPENSEARCH_JMX_HOST=127.0.0.2
export CASSANDRA_OPENSEARCH_JMX_PORT=21024
bin/cassandra-opensearch status
```

### 5. Start in order

Node 1 first, and wait for it. Cassandra reads `seed_provider` at daemon initialisation and
OpenSearch resolves its discovery settings while `Node` is being constructed — neither can be told
afterwards.

### macOS

Linux routes all of `127.0.0.0/8` to `lo` with no configuration. macOS does not:

```bash
sudo ifconfig lo0 alias 127.0.0.2 up
sudo ifconfig lo0 alias 127.0.0.3 up
```

Without this, the second node fails with `BindException: Can't assign requested address`.

---

## Worked example: coupled decommission

```bash
./create-cluster.sh -n 2 -d /tmp/demo
./cluster-control.sh -d /tmp/demo start
```

Put data in both clusters. Cassandra, at RF 1 so it genuinely has to stream on decommission:

```bash
export CQL="
  CREATE KEYSPACE demo WITH replication = {'class':'SimpleStrategy','replication_factor':1};
  CREATE TABLE demo.t (k int PRIMARY KEY, v text);
  INSERT INTO demo.t (k,v) VALUES (1,'hello');"
```

`cqlsh` is a Python program — `bin/cqlsh.py` plus the `cqlshlib` package, source files rather than
jars — so this distribution does not carry it, and `bin/cqlsh` exits 1 with "not available in this
distribution" until you tell it where a copy is. Either point it at a Cassandra installation:

```bash
export CASSANDRA_TOOLS_HOME=/path/to/apache-cassandra-5.0.x   # or a source tree
/tmp/demo/node1/bin/cqlsh 127.0.0.1 21012 -e "$CQL"
```

or run one out of a container, which needs no installation at all — `--network host` because the
node is on a loopback alias:

```bash
docker run --rm --network host cassandra:5 cqlsh 127.0.0.1 21012 -e "$CQL"
```

Any CQL driver does just as well; nothing below depends on which of the three you used.

OpenSearch, with more shards than nodes so relocation is real movement:

```bash
curl -XPUT '127.0.0.1:21015/demo' -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_shards":4,"number_of_replicas":0}}'
curl -XPOST '127.0.0.1:21015/demo/_doc?refresh=true' -H 'Content-Type: application/json' \
  -d '{"msg":"hello"}'
```

Retire node 2:

```bash
./cluster-control.sh -d /tmp/demo decommission 2
```

Watch the ordering in `/tmp/demo/node2/logs/cassandra-opensearch.log`:

```
[prepare-opensearch]      excluded 7b29362e-… from shard allocation
[await-shard-relocation]  relocating 2 of 2 shards
[await-shard-relocation]  relocated all 2 shards
[decommission-cassandra]  streaming ranges to the rest of the ring
[decommission-cassandra]  left the ring
Shutdown complete; exit code 0
```

OpenSearch finishes relocating **before** Cassandra starts streaming. Afterwards both the documents
and the CQL rows are intact on node 1:

```bash
./cluster-control.sh -d /tmp/demo ring      # one UN node
curl '127.0.0.1:21015/demo/_search?pretty'  # the document survived
```

`--force` is applied automatically below four nodes: `system_distributed` is RF 3, so Cassandra
refuses an unforced decommission that would take the ring under the replication factor.

---

## Troubleshooting

**A node fails to start with "address already in use".** Something holds one of its ports. Check
for a container published on `0.0.0.0` — it takes every loopback alias with it:
`docker ps` and `ss -ltnp | grep 21`.

**The second node forms its own OpenSearch cluster.** Its `cluster.initial_cluster_manager_nodes`
does not point at node 1, or it could not reach node 1 within the discovery timeout. Check
`_cat/nodes` on both: two one-node clusters is the signature.

**`nodetool` prints "DSE DB 5.0 requires Java 11 or higher" on JDK 21.** You are running the
fork's own script rather than the distribution's. The fork's `cassandra.in.sh` string-compares
versions (`"21.0.11" < "22"` is true) and rejects every JDK from 18 to 21. Use the tarball's
`bin/nodetool`, which works around it.

**`bin/cassandra-opensearch status` cannot connect.** It uses the *supervisor* JMX port, not
Cassandra's. Set `CASSANDRA_OPENSEARCH_JMX_HOST` and `CASSANDRA_OPENSEARCH_JMX_PORT`.

**OpenSearch logs nothing.** Its log is `logs/opensearch.log`, separate from Cassandra's
`logs/system.log` and the supervisor's `logs/cassandra-opensearch.log` — three logs, because there
are three logging configurations in the process by design.

**A decommission was refused and now indexes will not allocate.** This was a real bug, fixed: a
refused decommission used to leave the OpenSearch allocation exclusion in place. If you see it on
an older build, clear it with:

```bash
curl -XPUT '127.0.0.1:21015/_cluster/settings' -H 'Content-Type: application/json' \
  -d '{"transient":{"cluster.routing.allocation.exclude._name":null}}'
```

(21015 is node 1's REST port in this document's port plan. Do not paste `localhost:9200` here —
on a developer machine that is very often somebody else's OpenSearch.)
