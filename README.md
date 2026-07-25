# cassandra-opensearch

Runs an Apache Cassandra node and an OpenSearch node **inside a single JVM**, each in its own
isolated ClassLoader, under one supervisor with a coupled lifecycle.

Both are real servers, not embedded test harnesses, so the stock tooling works: `nodetool`,
`cqlsh`, and the OpenSearch REST API all talk to the running process unchanged.

## Requirements

- **JDK 21.** Not optional and not 17 — see [docs/JDK.md](docs/JDK.md) for why (OpenSearch 3.x
  ships Java 21 bytecode, which a 17 JVM cannot load).
- Maven 3.9+
- The Cassandra fork at `~/dev/cassandra`, built and installed to `~/.m2`:
  ```bash
  cd ~/dev/cassandra && JAVA_HOME=~/.sdkman/candidates/java/17.0.19-tem ant mvn-install
  ```

## Build

```bash
export JAVA_HOME=~/.sdkman/candidates/java/21.0.11-tem
mvn clean install          # unit tests
mvn verify                 # + integration tests against the assembled tarball
mvn verify -Pdocker        # + builds the Docker image
```

The tarball lands in `dist/target/cassandra-opensearch-1.0.0-SNAPSHOT-bin.tar.gz`.

## Run

```bash
tar xzf dist/target/cassandra-opensearch-*-bin.tar.gz
cd cassandra-opensearch-*
bin/cassandra-opensearch start
```

Then:

```bash
bin/nodetool status
bin/cqlsh
curl localhost:9200/_cluster/health?pretty
```

## Configuration

| File | Owns |
|---|---|
| `conf/cassandra-opensearch.yaml` | The supervisor: ports, directories, startup and decommission timeouts, coupling policy |
| `conf/cassandra.yaml` | Cassandra, stock format |
| `conf/opensearch.yml` | OpenSearch, stock format |
| `conf/jvm21-server.options` | JVM flags for both (see [docs/JDK.md](docs/JDK.md)) |

## Decommission

```bash
bin/cassandra-opensearch decommission [--timeout 30m] [--force]
```

This is the **only ordering-safe path**, and the ordering is the point: OpenSearch relocates its
shards off this node before Cassandra streams its ranges away and leaves the ring.

Running `nodetool decommission` directly still works — it is Cassandra's own operation — but it
does **not** order the OpenSearch relocation first, so shards resident on this node can be lost
when the process exits. The supervisor does not yet catch that case; see
[docs/KNOWN-GAPS.md](docs/KNOWN-GAPS.md) §1.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full lifecycle.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — isolation design, module map, lifecycle
- [docs/JDK.md](docs/JDK.md) — the JDK 17 / OpenSearch 3.x conflict and how it was resolved
- [docs/KNOWN-GAPS.md](docs/KNOWN-GAPS.md) — **what is not finished or not safe. Read this.**
- [docs/spikes/](docs/spikes/) — the de-risking spikes: how each server is embedded, and why
