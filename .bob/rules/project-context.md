# cassandra-opensearch — Bob project context

Read this at the start of every session in this workspace.

---

## Permissions

- **All git operations are allowed EXCEPT `git push`.**
  `git status`, `git diff`, `git add`, `git commit`, `git log`, `git branch`,
  `git checkout`, `git stash`, `git rebase`, `git merge`, `git reset` — all fine.
  `git push` (or any remote-writing operation) is **not allowed**; the user pushes manually.

- **All local file read and write under this directory is allowed.**
  No need to ask permission before reading, creating, editing, or deleting files
  anywhere under `sources/cassandra-opensearch/`.

---

## What this project is

A **co-location engine**: runs a full Apache Cassandra node and a full OpenSearch node
in the **same JVM**, each in an isolated `URLClassLoader`, under a shared lifecycle
supervisor. It is NOT a data synchronisation framework — the two servers do not exchange
data inside this process.

The data replication path (CQL write → OpenSearch document) is provided exclusively by
the **HCD fork** (`com.datastax.dse:dse-db-all:5.0.7.0-SNAPSHOT`, a private DataStax
Cassandra fork). That fork ships an `OpenSearchMutatorInterceptor` triggered by
`CREATE CUSTOM INDEX ... USING 'OpenSearchIndex'`. The source for that interceptor is
**NOT in this repository** — it is used as a black box.

---

## Module map

| Module | Role |
|---|---|
| `spi` | Zero-dep bridge contract (`EmbeddedService`, `ServiceContext`, etc.) |
| `bootstrap` | `IsolatedClassLoader`, TCCL-swapping proxy |
| `runtime-cassandra` | `EmbeddedService` over `CassandraDaemon` — needs HCD fork to build |
| `runtime-opensearch` | `EmbeddedService` over `org.opensearch.node.Node` |
| `server` | Supervisor, config, lifecycle, decommission, MBean |
| `cli` | `bin/*` launchers |
| `dist` | Tarball + Docker image (3 sub-modules to preserve classpath isolation) |
| `integration-tests` | Failsafe ITs driving the real assembled tarball as an external process |
| `hcd-tests` | End-to-end tests against a running HCD + OpenSearch docker-compose stack |
| `examples` | Multi-node shell scripts |

---

## CI scope

`build.yml` builds only: `spi,bootstrap,runtime-opensearch,server,cli,examples`.
Excluded (require the HCD fork or a live stack):
`runtime-cassandra`, `dist`, `integration-tests`, `hcd-tests`.

---

## Key technical facts

1. **JDK 21 only.** OpenSearch 3.x class files are major-65 (Java 21). JDK 17 cannot
   load them. `maven-enforcer-plugin` fails the build on anything but 21.

2. **ClassLoader isolation.** Each child's parent is the *platform* loader, not the
   application loader — parent-first delegation already excludes supervisor classes.
   Only `io.cassandraopensearch.spi.*` is delegated upward explicitly.

3. **TCCL pinning is load-bearing, not defensive.**
   Without it, `XContentBuilder.WRITERS` (a `static final`) permanently lacks its
   type writers. The node starts, reports healthy, indexes and searches — but
   `/_cluster/health`, `/_nodes/stats`, `/_cluster/stats` return HTTP 400.
   `SingleNodeServicesIT.theRestApiIsNotCorruptedByTheContextClassLoader()` is the canary.

4. **The SPI has zero dependencies — enforced by a build rule.**

5. **Decommission ordering is the key product invariant:** OpenSearch shards must relocate
   *before* Cassandra leaves the ring. `bin/cassandra-opensearch decommission` is the
   only ordering-safe path. `nodetool decommission` is explicitly unsafe.

---

## hcd-tests context

Targets a running stack started with:
```bash
HCD_VERSION=2.0.8-SNAPSHOT MAX_HEAP_SIZE=4G podman-compose -f docker-compose-full.yaml up -d
mvn test -pl hcd-tests -Phcd-tests --no-transfer-progress
```

### Key finding: `applyDefaultSchema` and UDT sub-fields

- **Top-level columns** (`full_name`, `house`) get `"type":"text"` with a `.keyword`
  sub-field at `CREATE CUSTOM INDEX` time — emitted as static properties.
- **UDT sub-fields** (`emails.address`, `phones.number`) are materialised only at first
  document insertion, hitting a `dynamic_template` (`strings_as_text`) that maps them
  to plain `text` — **no `.keyword` sub-field**.
- **Consequence:** `term` on `emails.address.keyword` returns 0 hits silently.
  Use `match_phrase` for UDT string sub-fields.

### Dual-read pattern added to all three test locations

After the session that introduced dual-read tests, three places demonstrate reading the
same data from both CQL and OpenSearch in one test:

1. **`runtime-opensearch/OpenSearchRestApiTest.indexesAndSearchesADocument()`** —
   after the search, does a direct `GET /books/_doc/1` and asserts `_source` matches
   what was indexed. Pure OpenSearch (no Cassandra), proves the embedded node's stored
   source is correct.

2. **`integration-tests/SingleNodeServicesIT`** — `dualRoundTripComparesTheSameDocument()`
   (`@Order(2)`, inserted between the existing CQL and OS round-trip tests):
   writes a row via CQL, reads it back over CQL, writes the same document directly to
   OpenSearch, searches it, and asserts the field values match across both protocols.
   Proves both servers in one JVM serve consistent reads of logically identical data.

3. **`hcd-tests/UdtProfileIndexingTest`** — `dualReadComparesOneStudentAcrossBothProtocols()`
   (`@Order(10)`, after all existing tests):
   fetches Harry Potter's Cassandra row by `user_id` over CQL and his OpenSearch
   document by `_id`, then asserts `full_name`, `house`, and `addresses[0].city`
   match. Proves the HCD interceptor replicates faithfully — no data mutation.

---

## Current branch

`rajeev/hcd-udt-indexing-tests` — PR in progress, do not push.
Owner: Rajeev Dave. AI-assisted commits carry the notice in `.pr-body.md`.
