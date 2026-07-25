# Remaining work

State of the project after one full review pass and one fix round, and what is left.

This is the **to-do list**. [KNOWN-GAPS.md](KNOWN-GAPS.md) is the companion: things that are
deliberately not fixed, or cannot be fixed here, which an operator needs to know about anyway. If
you are deciding what to work on next, read this; if you are deciding whether to run this in
production, read that one.

---

## Where things stand

Five reviewers examined the eighteen commits, each verifying findings by running code rather than
by inspection — probe programs for the concurrency claims, and deliberately-planted defects to
check the tests would catch them. They found **26 MAJOR issues**. All 26 are fixed, each pinned by
a test confirmed to fail against the old code.

The fix round also turned up four defects nobody had reported, found because a new test failed for
an unexpected reason. Two are worth naming:

- **`GCInspector` was never detached from the platform GC MXBeans.** It pinned the isolated
  ClassLoader for the life of the process, and after `stop()` kept handling collections into
  shut-down executors. The platform beans dispatch listeners in a plain loop with no per-listener
  `try`, so that exception killed every listener registered after it — meaning the *next* node's
  inspector silently died.
- **`removeShutdownHooks()` never ran when startup failed**, leaving Cassandra's JVM-global drain
  hook installed.

Test count went from 120 to **251** (229 unit + 22 integration). The `cli` module went from zero
tests to 47, which immediately caught a bug: `--jmx-port` with a bad value exited 1 with a stack
trace — the code a script reads as "the node is broken" rather than "you typed it wrong".

---

## P1 — do these before anyone relies on the thing

### 1. A second review pass has not been run against the fixes
One round of review and one round of fixes have happened. The fixes are individually tested, but
nobody has reviewed *them*. The concurrency changes in `Supervisor` especially — a shutdown budget,
a transition lock, a cancel-and-join — are exactly the kind of code that trades one race for
another.

### 2. `watch_external_decommission` still loses the race it was built for
Implemented and tested, but it is a catch-up: by the time Cassandra reports `LEAVING` the ring
transition has begun, and the supervisor has no way to hold anything up while shards move. See
[KNOWN-GAPS.md](KNOWN-GAPS.md) §1. Closing this properly needs either a Cassandra-side hook that
runs *before* the ring transition, or acceptance that only the supervisor-driven path is safe.

### 3. Nothing is authenticated
[KNOWN-GAPS.md](KNOWN-GAPS.md) §9. Every endpoint is loopback-only as shipped, so a default install
is not exposed — but the multi-node instructions require changing exactly those addresses, and at
that moment CQL, JMX (with RCE reachable through it) and the OpenSearch REST API are all open. At
minimum this should refuse to start, or warn loudly, when a non-loopback bind is configured without
authentication.

### 4. `ThreadAwareSecurityManager` blocks JDK 24
[KNOWN-GAPS.md](KNOWN-GAPS.md) §3. `System.setSecurityManager` is removed in 24, so this stack
cannot move past 21 without a fork patch making `install()` skippable. Needs to start now, not when
21 goes out of support.

---

## P2 — test gaps that matter

### 5. Fixed test ports, and a static node counter shared across classes
`runtime-cassandra`'s `TestNode` bases ports on `17000/19042/17199 + 10 × NODES_STARTED`, a static
counter shared by every test class in one surefire JVM — so a class's ports depend on how many
nodes earlier classes started. `runtime-opensearch` hardcodes 19200–19330. Nothing probes for
bindability first.

This is not hypothetical: running two builds of this repo concurrently produces `BindException`
failures that look like product defects. It cost real time during the fix round, twice, and both
agents initially misattributed it to an unrelated container. Allocate ports from the kernel, or at
least probe and skip.

### 6. Cross-class shared state in tests
Three `server` test classes register the same `io.cassandraopensearch:type=Supervisor` on the
JVM-global platform MBean server, and `Supervisor.registerMBean` swallows a duplicate registration
into a log line — so a leak from one class becomes a confusing failure in another.
`RingStateWatcherTest` scans every live JVM thread by name while other classes are running real
watchers. `TestNode` sets JVM-global `cassandra.*` properties and never clears them.

### 7. Negatives proved by a fixed 500 ms sleep
Five assertions in `ExternalDecommissionTest` prove "this did *not* happen" by sleeping and
looking. On a loaded machine that yields a false **pass** — the safe direction, but it means those
five can silently stop testing anything. Assert on a positive signal where one exists.

### 8. Untested paths
- The unforced (N≥3) decommission. Two nodes prove the ordering; the unforced path needs three,
  because `system_distributed` is RF 3.
- `bin/cqlsh`, `bin/opensearch-plugin`, `bin/opensearch-keystore` — syntax-checked only. They need
  an external installation the build cannot assume.
- The Docker image is not built under `mvn verify` (only under `-Pdocker`), so nothing in CI would
  catch a Dockerfile regression.
- `Locale.ROOT` in `CassandraService.details()` and the `stateUUID()` snapshot key: both fixed,
  neither tested, for reasons recorded in the fix report — `Locale.setDefault` is JVM-global and
  the suite shares nodes, and reproducing a lost cluster manager needs a multi-node OpenSearch
  fixture.
- `bothServersRunInOneJvm` — the product's headline claim — is skipped entirely on non-Linux
  because it reads `/proc`. The second, `/proc`-independent assertion sits after the `assumeTrue`
  and is skipped with it; it should move above.

### 9. The IT skip mechanism is a blunt instrument
`integration-tests/pom.xml` skips ITs whenever `${session.goals}` contains the substring
`install`, so `mvn clean install verify` is green **without running a single IT**. Deliberate and
documented, but there is no escape hatch for running ITs under `install`.

---

## P3 — worth doing, not urgent

10. **`DecommissionException` crosses the MBean interface**, whose javadoc claims every type
    crossing it is a JDK type. It works only because the CLI's classpath happens to contain the
    server jar. Throw a JDK type carrying the message, or return a structured result.
11. **No way to cancel a decommission.** `DecommissionCoordinator.cancel()` is reachable only from
    `shutdown()`. Ctrl-C on the CLI drops the RMI connection but the node still leaves the ring.
    The whole `isCancelled()` plumbing has no operator-facing caller.
12. **`CassandraService.awaitDecommissionReady` is unreachable** from the supervisor path — the
    coordinator scopes that phase to OpenSearch, so Cassandra's "a `nodetool` decommission is
    already in flight" guard never runs.
13. **A successful decommission leaves a dead entry** in
    `cluster.routing.allocation.exclude._name` forever — one per retired node.
14. **`FederatedMBeanServer`'s javadoc says writes are not routed**; they are, by domain, like
    everything else. The safety property holds; the sentence does not.
15. **`ARCHITECTURE.md`'s layout block** says `cassandra-opensearch-1.0.0/` where the build produces
    `-1.0.0-SNAPSHOT`, and omits `conf/logback-tools.xml`.
16. **Add `shellcheck` to the build.** It is not installed here, and `bash -n` catches only syntax.
    Several of the shell defects the review found — guards that cannot abort, unquoted expansions —
    are exactly what shellcheck reports.

---

## Work this creates for the Cassandra fork

Found by the spikes and confirmed in use. None are worked around in a way that removes the need to
fix them upstream.

1. **`bin/cassandra.in.sh` rejects every JDK from 18 to 21** — it string-compares `"21.0.11" < "22"`
   — killing `nodetool`, `cqlsh` and `sstable*`. Worked around here via `CASSANDRA_INCLUDE`.
2. **No `conf/jvm21-server.options`.** One is synthesized in `dist/`; it belongs upstream.
3. **`GCInspector` builds `gcStates` from a different MBeanServer than the one it registers its
   listener on**, which NPEs on every GC behind a private server. It also never unregisters
   itself — see the leak described above.
4. **`ThreadAwareSecurityManager.install()` is not skippable.** JDK 24 blocker.
5. **`org.apache.cassandra.nodes.Nodes` has no `shutdown()`**, so its `nodes-info-persistence`
   thread cannot be stopped and pins the ClassLoader. Fork-specific; upstream dtest does not cover it.
6. **Unnamespaced system properties** — `ssl.enable`, `default.*`,
   `compaction_rate_limit_granularity_in_kb`, `check_data_resurrection_heartbeat_period_milli`.
7. **The generated `dse-db-all` POM has unresolved `systemPath` entries** (`${jmc5.path}`,
   `${visualvm.path}`), which print `[ERROR]` lines on every build of every downstream project.

---

## Not planned

Recorded so nobody re-derives the reasoning:

- **Making the unsupervised `nodetool decommission` path fully safe.** It cannot be done from
  outside Cassandra; see §2 above.
- **Waiting for shard relocation and then stopping OpenSearch on the unsupervised path.** It would
  run a 30-minute wait on a background thread while the shutdown hook and CLI can drive the same
  service — the concurrent lifecycle the SPI forbids — and stopping OpenSearch early destroys
  exactly the shards the exclusion exists to save.
- **Making the isolated ClassLoaders collectable.** Two of the roots are inside the fork and JNA.
  Irrelevant while each service is loaded once per process; the mitigation is to restart a service
  *inside* its existing loader.
- **Adopting either server's `io.netty.*` tuning wholesale.** They contradict each other. See
  [KNOWN-GAPS.md](KNOWN-GAPS.md) §6 for what ships and who pays for it.
