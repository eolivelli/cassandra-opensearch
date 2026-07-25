# De-risking spike — embedding the Cassandra fork in an isolated ClassLoader on JDK 21

Runtime: JDK 21.0.11 Temurin (`/home/eolivelli/.sdkman/candidates/java/21.0.11-tem`)
Server: `com.datastax.dse:dse-db-all:5.0.7.0-SNAPSHOT`, built from `/home/eolivelli/dev/cassandra` (never modified)
Spike trees: `scratchpad/spike-cassandra` (shared) and `scratchpad/spike2` (private copy used for the multi-node runs)
Evidence logs: `run4.log`, `nodetool4.out`, `run-2node-2.log`, `nodetool-2node.out`, `run-decom.log`

> **Note on the two spike trees.** A second agent was working concurrently in
> `scratchpad/spike-cassandra` during this run (it was bisecting JVM flags and rewrote
> `TwoNodeMain.java`, `jvm21-server.options` and held ports). To keep results reproducible, all
> multi-node evidence in this report was produced from a private copy, `scratchpad/spike2`, on a
> non-overlapping port range. `spike2` is the authoritative tree for everything below.

---

## 1. VERDICT

| Question | Verdict |
|---|---|
| Starts in an isolated ClassLoader on JDK 21 | **YES** |
| Stock `bin/nodetool` manages it from outside the JVM | **YES — with one caveat**: the fork's shell scripts reject JDK 21 outright. One-line fix, see §5. |
| CQL round trip from outside the JVM | **YES** |
| Decommission works | **YES-WITH-CAVEATS** — *proven* between two in-JVM nodes (`decommission: OK`, 22.1 s, ring converges to 1, data survives). Impossible on 1 node; blocked on 2 nodes until keyspace RFs are lowered (stock `system_distributed` is RF 3). See §9. |
| Two isolated nodes in one JVM | **YES** — both `NORMAL`, gossiping, streaming, independently manageable by stock nodetool on separate JMX ports. No JNA or netty native-library collision. |
| Clean shutdown, JVM exits, no hung non-daemon threads | **YES** — `JVM-EXIT-CODE=0`; two-node run ends with `(none -- clean)`. |
| Isolated ClassLoader can be GC'd after a clean stop | **NO** — ~11 daemon threads per node still root it. Two of them (`nodes-info-persistence`, `JNA Cleaner`) have no shutdown API today. See §10. |

Headline: the architecture works. Cassandra runs, is manageable, serves CQL, and stops cleanly
inside a child ClassLoader on JDK 21, and two of them coexist in one JVM. The unsolved item is
metaspace reclamation (loader GC), which matters for long-lived restart-in-place, not for the
basic co-hosting design.

---

## 2. Environment caveat: ports

Two pre-existing docker containers squat the default ports on this box:

```
1f998121bf81  cassandra:5.0                       0.0.0.0:9042->9042/tcp   dbui-cassandra
262596a228ca  opensearchproject/opensearch:3.6.0  0.0.0.0:9200->9200/tcp   dbui-opensearch
```

A `0.0.0.0` bind blocks every loopback alias, so 9042 was unusable and the spike used offset
ports. Nothing in the design depends on the numbers.

| role | single-node run | 2-node run | decom run |
|---|---|---|---|
| storage/gossip | 17000 | 27000 (both nodes, distinguished by address) | 37000 |
| native/CQL | 19042 | 29042 | 39042 |
| JMX | 17199 | 27199 / 27299 | 37199 / 37299 |

---

## 3. The isolated ClassLoader and its shared-class predicate

`spike/bridge/IsolatedClassLoader.java`. Parent-last by construction: `super(urls, null)` — the
parent is **null**, so nothing leaks in from the application loader except what the predicate
explicitly routes there.

```java
public static final String BRIDGE_PACKAGE = "spike.bridge.";

public static final Predicate<String> DEFAULT_SHARED = name ->
       name.startsWith(BRIDGE_PACKAGE)
    || isPlatformClass(name);

public static boolean isPlatformClass(String name)
{
    if (name.startsWith("java.") || name.startsWith("jdk.internal."))
        return true;
    if (!(name.startsWith("javax.") || name.startsWith("jdk.") || name.startsWith("sun.")
          || name.startsWith("com.sun.") || name.startsWith("org.w3c.") || name.startsWith("org.xml.")
          || name.startsWith("org.ietf.") || name.startsWith("netscape.") || name.startsWith("oracle.")
          || name.startsWith("com.oracle.") || name.startsWith("org.graalvm.")))
        return false;
    return PLATFORM_CACHE.computeIfAbsent(name, n -> {
        try { Class.forName(n, false, ClassLoader.getPlatformClassLoader()); return TRUE; }
        catch (Throwable t) { return FALSE; }
    });
}
```

**The single most important design decision in the spike.** dtest's `InstanceClassLoader` shares
by *package prefix* (`javax.`, `sun.`, `com.sun.`…). That is safe in dtest only because there the
parent loader already holds the whole Cassandra classpath, so a prefix miss is harmless. Here the
application loader deliberately does **not** have Cassandra's dependencies, and several of those
dependencies squat in JDK-looking packages:

| package | actually shipped by |
|---|---|
| `javax.annotation.Nullable` | `jsr305-3.0.0.jar` |
| `javax.inject.*` | `javax.inject-1.jar` |
| `com.sun.jna.*` | `jna-5.13.0.jar` |

So "is this a platform class?" is answered by **asking the platform ClassLoader**, not by trusting
the name. A prefix rule sends `javax.annotation.Nullable` to the app loader and the node dies in
`YamlConfigurationLoader` with `NoClassDefFoundError`.

> **Correction to the brief.** The `ClassNotFoundException: javax.annotation.Nullable` that stopped
> the earlier run was *not* a missing jar. `jsr305-3.0.0.jar` was on the isolated classpath the
> whole time (verified: present in both the Maven tree and the fork's `lib/`). It was this
> predicate bug. Adding jars would not have fixed it.

Isolation is asserted at startup and holds:

```
child   StorageService -> IsolatedClassLoader{cassandra, 116 urls}
app classloader can see org.apache.cassandra.* : false   (expected false)
bridge same class on both sides: true   (expected true)
child   io.netty.channel.EventLoopGroup -> IsolatedClassLoader{cassandra, 116 urls}
app     io.netty.channel.EventLoopGroup -> jdk.internal.loader.ClassLoaders$AppClassLoader   (driver's netty)
netty is distinct across loaders: true   (expected true)
logback private to child: IsolatedClassLoader{cassandra, 116 urls}
```

The only shared type is the bridge interface `spike.bridge.ICassandraNode`, whose whole signature
is JDK types (`Map<String,String>`, `String`), so no Cassandra class ever crosses the boundary.

---

## 4. Classpath: fork `lib/` vs Maven tree — recommendation

Measured, not assumed. Normalising both lists by artifact name:

* Maven tree (`dependency:build-classpath` on `dse-db-all`): **115 jars**
* fork `lib/*.jar`: **108 jars**
* jars in `lib/*.jar` **missing** from the Maven tree: **none**
* extra in the Maven tree: `dse-db-all` itself plus 6 foreign-platform netty classifier jars
  (`osx-aarch_64`, `osx-x86_64`, `windows-x86_64`, `linux-riscv64`) — harmless

**But there is a gap the naive comparison hides.** `bin/cassandra.in.sh` builds its classpath from
`lib/*.jar` **and, separately, `lib/$(uname -m)/*.jar`**:

```sh
platform=$(uname -m)
if [ -d "$CASSANDRA_HOME"/lib/"$platform" ]; then
    for jar in "$CASSANDRA_HOME"/lib/"$platform"/*.jar ; do CLASSPATH="$CLASSPATH:${jar}"; done
fi
```

`lib/x86_64/` holds `AmazonCorrettoCryptoProvider-2.2.0-linux-x86_64.jar` (and `lib/aarch64/` the
arm equivalent). **That jar is not in the Maven tree and not even in the local m2 repo.** The stock
`cassandra.yaml` enables it by default:

```yaml
crypto_provider:
  - class_name: org.apache.cassandra.security.DefaultCryptoProvider
    parameters:
      - fail_on_missing_provider: "false"
```

so every spike run logged:

```
WARN org.apache.cassandra.security.AbstractCryptoProvider -- com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider
is not on the class path! Check node's architecture (`uname -m`) is supported, see lib/<arch> subdirectories.
```

It is only a warning because `fail_on_missing_provider: "false"`, so the node started anyway and
fell back to the JRE provider. Set it to `"true"` and the Maven-tree classpath would fail to boot.

**Recommendation: use the Maven tree, plus an explicit `lib/$(uname -m)/*.jar` entry** (or pin ACCP
as a real Maven dependency, or set `crypto_provider` to
`org.apache.cassandra.security.JREProvider` and drop it entirely). The Maven tree is reproducible
from a POM, versioned, and CI-friendly, and it is a strict superset of `lib/*.jar`; globbing
`lib/` instead requires a built distribution on disk and buys nothing except the arch jar, which
you can add in one line. The brief's suspicion that the Maven list omits ordinary jars is **not**
borne out — the failure that motivated it was the ClassLoader predicate (§3), not a missing jar.

> **Co-hosting note.** `DefaultCryptoProvider` installs a JCE provider through
> `java.security.Security.addProvider` and removes it through `Security.removeProvider`. That
> registry is **JVM-global** and shared with OpenSearch — see §11.

## 5. Stock `nodetool`: the fork's scripts reject JDK 21

Before the node was even involved, every nodetool invocation failed:

```
$ bin/nodetool -p 17199 status
DSE DB 5.0 requires Java 11 or higher.
```

Root cause is `bin/cassandra.in.sh`, which string-compares version strings:

```sh
JAVA_VERSION=22
if   [ "$short" = "11" ]           ; then JAVA_VERSION=11
elif [ "$JVM_VERSION" \< "17" ]    ; then echo "DSE DB 5.0 requires Java 11 or higher."; exit 1
elif [ "$short" = "17" ]           ; then JAVA_VERSION=17
elif [ "$JVM_VERSION" \< "22" ]    ; then echo "DSE DB 5.0 requires Java 11 or higher."; exit 1
fi
```

For JDK 21 `JVM_VERSION="21.0.11"`, `short="21"`: it is not 11, not `< "17"` as a string, not 17,
but `"21.0.11" < "22"` is true — so it exits 1. **The fork supports exactly 11 / 17 / 22; every
JDK 18–21 is rejected**, and the message is misleading.

Fix used by the spike (numeric ladder), applied through the stock script's own supported
`CASSANDRA_INCLUDE` hook so nothing under `/home/eolivelli/dev/cassandra` was touched:

```sh
major=$(echo "$jvmver" | cut -d. -f1)
if   [ "$major" -lt 11 ] ; then echo "requires Java 11 or higher."; exit 1
elif [ "$major" -lt 17 ] ; then JAVA_VERSION=11
elif [ "$major" -lt 22 ] ; then JAVA_VERSION=17   # 17..21 share the same JPMS opens/exports set
else                            JAVA_VERSION=22
fi
```

Usage: `CASSANDRA_INCLUDE=$PWD/spike-cassandra.in.sh bin/nodetool -p 17199 status`.

The real project needs this ladder fixed in `bin/cassandra.in.sh`, and should add
`conf/jvm21-server.options` / `conf/jvm21-clients.options`. JDK 21 needs no JPMS flags beyond the
17 set, so mapping 18–21 onto the `jvm17-*` files is sufficient and was validated by running.

---

## 6. `jvm21-server.options` — validated by actually starting a node

Derived from `conf/jvm17-server.options` plus the JPMS entries `conf/jvm22-server.options` adds,
minus everything 22-only. Every line below was in force for the runs that produced §7–§9.

```
-XX:ThreadPriorityPolicy=1
-XX:+UseThreadPriorities

### G1
-XX:+UseG1GC
-XX:+ParallelRefProcEnabled
-XX:MaxTenuringThreshold=2
-XX:G1HeapRegionSize=16m
-XX:+UnlockExperimentalVMOptions
-XX:G1NewSizePercent=50
-XX:G1RSetUpdatingPauseTimePercent=5
-XX:MaxGCPauseMillis=500
-XX:InitiatingHeapOccupancyPercent=70

### JPMS
-Djdk.attach.allowAttachSelf=true
-Djava.security.manager=allow

--add-exports java.base/jdk.internal.misc=ALL-UNNAMED
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports java.base/java.lang.ref=ALL-UNNAMED
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
--add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED
--add-exports java.management/com.sun.jmx.remote.security=ALL-UNNAMED
--add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED
--add-exports java.rmi/sun.rmi.server=ALL-UNNAMED
--add-exports java.sql/java.sql=ALL-UNNAMED
--add-exports jdk.unsupported/sun.misc=ALL-UNNAMED
--add-exports jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED

--add-opens java.base/java.io=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.module=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.net=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/java.nio.charset=ALL-UNNAMED
--add-opens java.base/java.nio.file.attribute=ALL-UNNAMED
--add-opens java.base/java.nio.file.spi=ALL-UNNAMED
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.util.concurrent=ALL-UNNAMED
--add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens java.base/java.util.concurrent.locks=ALL-UNNAMED
--add-opens java.base/jdk.internal.loader=ALL-UNNAMED
--add-opens java.base/jdk.internal.math=ALL-UNNAMED
--add-opens java.base/jdk.internal.module=ALL-UNNAMED
--add-opens java.base/jdk.internal.ref=ALL-UNNAMED
--add-opens java.base/jdk.internal.reflect=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens jdk.compiler/com.sun.tools.javac=ALL-UNNAMED
--add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED

### Netty
-Dio.netty.tryReflectionSetAccessible=true
-Dio.netty.allocator.useCacheForAllThreads=true
-Dio.netty.allocator.maxOrder=11

### SIMD (jvector / lucene)
--add-modules jdk.incubator.vector
--enable-native-access=ALL-UNNAMED
```

Deliberate deltas from `jvm17-server.options`:

* **dropped** `--add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED` — that package no longer
  exists in `java.base` on JDK 21, and keeping it makes every JVM start print
  `WARNING: package jdk.internal.util.jar not in java.base`.
* `-XX:ThreadPriorityPolicy=1` prints a warning as non-root; harmless, kept for parity with the
  stock file.
* `--add-modules jdk.incubator.vector` prints `WARNING: Using incubator modules`. Unavoidable.

### Flags added programmatically by `conf/cassandra-env.sh`

These are **not** in any `.options` file and must be reproduced by the embedding host:

| flag | note |
|---|---|
| `-XX:CompileCommandFile=$CASSANDRA_CONF/hotspot_compiler` | JVM-global |
| `-javaagent:$CASSANDRA_HOME/lib/jamm-0.4.0.jar` | JVM-global; the spike ran **without** it, using `-Djdk.attach.allowAttachSelf=true` so jamm self-attaches |
| `-Djava.library.path=$CASSANDRA_HOME/lib/sigar-bin` | JVM-global, single-valued — see §11 |
| `-Dcassandra.jmx.local.port=$JMX_PORT` + `-Dcom.sun.management.jmxremote.authenticate=false` | the spike deliberately leaves these **unset** so `CassandraDaemon.maybeInitJmx()` no-ops and the embedder owns JMX |
| `-Xms/-Xmx/-Xmn/-XX:MaxDirectMemorySize`, `-XX:ParallelGCThreads`, `-XX:ConcGCThreads`, `-XX:+UseCondCardMark` | sized from host RAM/CPU |
| `-Xlog:gc…:file=${CASSANDRA_LOG_DIR}/gc.log` | JVM-global, single GC log destination |
| `-XX:HeapDumpPath=…` | JVM-global |

---

## 7. Real captured `nodetool` output (stock `bin/nodetool`, separate JVM)

All 14 commands driven exited **0**. Full transcript in `nodetool4.out`.

```
########## nodetool -p 17199 status ##########
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address    Load        Tokens  Owns (effective)  Host ID                               Rack 
UN  127.0.0.1  140.73 KiB  4       100.0%            71626770-ebdb-479a-a91c-72ee6165e37c  rack1

########## nodetool -p 17199 info ##########
ID                     : 71626770-ebdb-479a-a91c-72ee6165e37c
Gossip active          : true
Native Transport active: true
Load                   : 140.73 KiB
Uncompressed load      : 175.28 KiB
Generation No          : 1784962426
Uptime (seconds)       : 17
Heap Memory (MB)       : 193.45 / 2048.00
Off Heap Memory (MB)   : 0.00
Data Center            : datacenter1
Rack                   : rack1
Exceptions             : 0
Key Cache              : entries 0, size 0 bytes, capacity 52 MiB, 0 hits, 0 requests, NaN recent hit rate, 14400 save period in seconds
Row Cache              : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
Counter Cache          : entries 0, size 0 bytes, capacity 26 MiB, 0 hits, 0 requests, NaN recent hit rate, 7200 save period in seconds
Percent Repaired       : 100.0%
Token                  : (invoke with -T/--tokens to see all 4 tokens)
Bootstrap state        : COMPLETED
Bootstrap failed       : false
Decommissioning        : false
Decommission failed    : false
```

Also verified working: `version`, `describecluster`, `gossipinfo`, `netstats`, `tpstats`,
`getlogginglevels`, `compactionstats`, `ring`, `statusgossip`, `statusbinary`, `flush`, `gcstats`.

### Why this needed more than dtest's `IsolatedJmx`

`IsolatedJmx` is built for in-JVM clients. Three changes were required for an **out-of-process**
`nodetool`:

1. **No RMI client socket factory.** dtest passes its own `RMIClientSocketFactoryImpl`. That object
   is serialised inside the stub to the remote client, where the class does not exist —
   `ClassNotFoundException` in nodetool's JVM. Pass `null` and get the JDK default.
2. **`java.rmi.server.hostname` must be pinned** before export, or the stub advertises a
   non-loopback address and nodetool dials the wrong interface.
3. **The MBeanServer handed to `RMIConnectorServer` must be federated.** The node owns a *private*
   `MBeanServer` (via `-Dorg.apache.cassandra.mbean_registration_class`), but `NodeProbe.connect()`
   immediately does `ManagementFactory.newPlatformMXBeanProxy(conn, MEMORY_MXBEAN_NAME, …)` and
   `RUNTIME_MXBEAN_NAME`, and `nodetool info` reads heap and uptime from them. A bare
   `MBeanServerFactory.createMBeanServer()` has neither. `FederatedMBeanServer` is a dynamic proxy
   routing `java.*` / `com.sun.management` / `JMImplementation` to the JVM platform server and
   everything else to the node's private one, and unioning wildcard `queryNames`/`queryMBeans`.

That federation also fixed a startup-noise bug dtest never sees: `GCInspector`'s constructor builds
its `gcStates` map from `MBeanWrapper.instance.queryNames("java.lang:type=GarbageCollector,*")`
while `register()` adds its listener to the *platform* server. With a bare private server the query
returns nothing, `gcStates` stays empty, and **every GC** throws
`NullPointerException: Cannot read field "assumeGCIsPartiallyConcurrent" because "gcState" is null`.
dtest avoids it only by never calling `GCInspector.register()`; a real `CassandraDaemon.setup()`
does. Count of that NPE: **4 in the pre-fix run, 0 after**.

---

## 8. Real captured CQL round trip

DataStax java driver 4.18.1, loaded by the **application** ClassLoader (so this is a genuine
out-of-loader client speaking the native protocol over TCP), against `127.0.0.1:19042`:

```
connected: SpikeCluster
CREATE KEYSPACE ok
CREATE TABLE ok
INSERT ok
ROW  k=hello  v=isolated-classloader  n=42
driver netty loaded by: jdk.internal.loader.ClassLoaders$AppClassLoader@76ed5528
```

The last line is the point: the driver's netty is the app loader's, the server's netty is the
child's, and they interoperate over a socket with no class sharing at all.

---

## 9. Two nodes in one JVM, and decommission

### Two nodes: YES

`spike/TwoNodeDecom.java` / `TwoNodeMain.java` start two nodes in two `IsolatedClassLoader`s:

```
*** node1 up in 4.3s on 127.0.0.1 (jmx 127.0.0.1:27199)
node1: mode=NORMAL joined=true live=[127.0.0.1]
*** node2 up in 18.0s on 127.0.0.2 (jmx 127.0.0.1:27299)
node2: mode=NORMAL joined=true live=[127.0.0.1, 127.0.0.2]

StorageService class n1 == n2 ? false   (expected false)
n1 loader = IsolatedClassLoader{node1, 116 urls}
n2 loader = IsolatedClassLoader{node2, 116 urls}
n1 sees ring: [127.0.0.1, 127.0.0.2]
n2 sees ring: [127.0.0.1, 127.0.0.2]
```

Stock nodetool manages each independently (`nodetool-2node.out`):

```
########## nodetool -p 27199 status ##########
UN  127.0.0.2  35.44 KiB   4  100.0%  75ce7074-4d9c-4a50-acb4-6355c2e824b7  rack1
UN  127.0.0.1  118.38 KiB  4  100.0%  8dcb2b0b-5d28-460a-91f5-dbc539288119  rack1

########## nodetool -p 27199 describecluster ##########
	Live: 2 ... Data Centers: datacenter1 #Nodes: 2 #Down: 0
	Database versions: 5.0.7.0-SNAPSHOT: [127.0.0.1:27000, 127.0.0.2:27000]
```

Two constraints fall straight out of "system properties are JVM-global":

1. **Nodes must be started sequentially.** `DatabaseDescriptor` reads `cassandra.config` /
   `cassandra.storagedir` once per *classloader*, during `daemonInitialization()`. Set the globals,
   start node N, then rewrite them for node N+1. They cannot boot concurrently.
2. **The two JMX servers must share one address and differ by port.** `java.rmi.server.hostname` is
   read once per JVM and cached in RMI's static `TCPEndpoint` local-host, so two RMI servers in one
   JVM cannot advertise two different hostnames. Storage/native ports, by contrast, differ by
   *address* and can keep identical port numbers (exactly like ccm).

### Decommission

**Single node — rejected by design**, captured verbatim:

```
StorageService.decommission(false) -> java.lang.UnsupportedOperationException: no other normal nodes in the ring; decommission would be pointless
StorageService.decommission(true)  -> java.lang.UnsupportedOperationException: no other normal nodes in the ring; decommission would be pointless
operationMode after                -> NORMAL
```

Note `force=true` does **not** help — this check precedes the force check. `drain()` is the
single-node equivalent and works (see §10).

**Two nodes, stock config — still rejected**, for a different and more interesting reason:

```
node2 decommission -> java.lang.UnsupportedOperationException: Not enough live nodes to maintain
  replication factor in keyspace system_distributed (RF = 3, N = 2). Perform a forceful decommission to ignore.
node2 mode after   -> DECOMMISSION_FAILED
node1 live nodes   -> [127.0.0.1, 127.0.0.2]
```

The stock `cassandra.yaml` creates `system_distributed` at RF 3 and `system_traces` at RF 2, so
going 2 → 1 node breaks both. Consequences for the real integration test:

* a **3-node** cluster decommissions cleanly with stock RFs, or
* a **2-node** cluster works if the test first lowers every keyspace RF to 1, or
* `decommission(force=true)` bypasses the RF guard.

Note also the terminal state on failure is `DECOMMISSION_FAILED`, not `NORMAL` — a test must
account for that.

**Two nodes, RFs lowered first — SUCCEEDS.** Full transcript in `run-decom.log`:

```
================ B. CQL: user keyspace at RF=2, write a row (both nodes hold it) ================
  ROW k=before v=written-while-2-nodes

================ C. FIRST DECOMMISSION ATTEMPT (stock RFs) -- EXPECTED TO FAIL ================
node2 decommission(false) -> java.lang.UnsupportedOperationException: Not enough live nodes to maintain
   replication factor in keyspace system_distributed (RF = 3, N = 2). Perform a forceful decommission to ignore.
node2 mode                -> DECOMMISSION_FAILED

================ D. LOWER EVERY RF SO A 1-NODE RING IS LEGAL ================
  ALTER KEYSPACE system_distributed -> RF 1
  ALTER KEYSPACE system_traces      -> RF 1
  ALTER KEYSPACE system_auth        -> RF 1
  ALTER KEYSPACE decom              -> RF 1

================ E. SECOND DECOMMISSION ATTEMPT -- EXPECTED TO SUCCEED ================
node2 decommission(false) -> decommission: OK   (22.1s)
node2 mode                -> DECOMMISSIONED
node1 ring after decom    -> [127.0.0.1]
node1 mode                -> NORMAL

================ F. DATA SURVIVED ON THE REMAINING NODE? ================
  ROW k=before v=written-while-2-nodes
  ROW k=before v=written-while-2-nodes
  ROW k=after  v=written-after-decommission
```

Confirmed from outside the JVM by stock nodetool against node1 afterwards
(`nodetool-after-decom.out`):

```
########## nodetool -p 37199 status   (node1, AFTER node2 decommissioned) ##########
Datacenter: datacenter1
=======================
--  Address    Load        Tokens  Owns (effective)  Host ID                               Rack 
UN  127.0.0.1  171.93 KiB  4       100.0%            7e687b58-6982-4642-a6ac-e69b1714031f  rack1

########## nodetool -p 37199 describecluster ##########
	Schema versions:
		9d42dd15-7140-37f5-b15f-22750d435d53: [127.0.0.1]
		UNREACHABLE: [127.0.0.2]
	Stats for all nodes:  Live: 1  ...  Unreachable: 1
	Data Centers: datacenter1 #Nodes: 1 #Down: 0
```

So: **a genuine decommission integration test is feasible in-JVM.** The recipe is two isolated
loaders on 127.0.0.1 / 127.0.0.2, an RF reduction (or a third node), then
`StorageService.instance.decommission(false)`. Streaming, token handoff and ring convergence all
work between two loaders in the same process. Note `describecluster` keeps reporting the departed
node as `UNREACHABLE` in the schema-version section for a while — a test should assert on
`nodetool status` / `getLiveNodes()`, not on that line.

---

## 10. Shutdown, cleanliness and ClassLoader GC

### Clean exit: YES

```
===== JVM-EXIT-CODE=0 =====
```

The per-service teardown in `CassandraNodeImpl.shutdown()` runs to completion, all steps `ok`:

```
[node] client transports destroyed
[node] drained
[node] JMX down
[node] org.apache.cassandra.net.MessagingService.shutdown ok
[node] org.apache.cassandra.concurrent.Stage.shutdownAndWait ok
[node] org.apache.cassandra.concurrent.ScheduledExecutors.shutdownNowAndWait ok
[node] org.apache.cassandra.db.ColumnFamilyStore.shutdownExecutorsAndWait ok
[node] org.apache.cassandra.utils.memory.BufferPools.shutdownLocalCleaner ok
[node] org.apache.cassandra.utils.concurrent.Ref.shutdownReferenceReaper ok
[node] org.apache.cassandra.io.sstable.format.SSTableReader.shutdownBlocking ok
[node] SharedExecutorPool.SHARED down
[node] commitlog down
[node] logback context stopped
```

In the two-node run, **both** nodes shut down this way and the final non-daemon thread scan was
`(none -- clean)`. The one non-daemon straggler in the single-node run is netty's
`globalEventExecutor-2-N`, which self-terminates after a ~1 s quiet period, so it never actually
hangs the JVM. dtest handles it explicitly with
`GlobalEventExecutor.INSTANCE.awaitInactivity(1, MINUTES)` — worth copying.

### Shutdown hooks: Cassandra registers JVM-global ones, and they must be removed

Yes, and there are two independent families:

| site | what it does |
|---|---|
| `JVMStabilityInspector.registerShutdownHook(...)` → `Runtime.getRuntime().addShutdownHook(hook)` | the drain-on-shutdown hook installed by `StorageService.initServer()` |
| `PathUtils` (`Runtime.getRuntime().addShutdownHook(onExitThread)`) | `deleteOnExit` bookkeeping |
| `ThreadsFactory.addShutdownHook(...)` | helper, currently unused in the fork |

`JVMStabilityInspector.removeShutdownHooks()` is the supported removal API and the spike calls it
immediately after `activate()` — exactly what `Instance.startup()` does (`Instance.java:734`,
`:873`). `PathUtils` has its own pair, `runOnExitThreadsAndClear()` / `clearOnExitThreads()`, which
dtest calls at `Instance.java:958/962` with the comment *"Make sure any shutdown hooks registered
for DeleteOnExit are released to prevent references to the instance class loaders from being
held."* **The spike does not yet call these** — see the GC section.

### Cassandra must never call `System.exit` — two separate paths, both handled

1. `CassandraDaemon.exitOrFail()` → `System.exit(code)` and `deactivate()` → `System.exit(0)`.
   Both are guarded by `runManaged`. The spike constructs
   `new CassandraDaemon(true)`, so `exitOrFail` throws a `RuntimeException` instead.
2. **`JVMStabilityInspector.Killer.killJVM()` → `System.exit(100)`.** This is the dangerous one: it
   fires on `disk_failure_policy: die`, `commit_failure_policy: die`, `OutOfMemoryError`, `FSError`,
   corrupt sstables — i.e. on ordinary operational faults — and it is **not** covered by
   `runManaged`. The stock `cassandra.yaml` ships `disk_failure_policy: stop` and
   `commit_failure_policy: stop`, but any change to `die` would take OpenSearch down with it.
   Mitigation, installed before `activate()` via the supported `replaceKiller` hook (dtest does the
   same with `InstanceKiller`):

```java
Class<?> jvmStability = Class.forName("org.apache.cassandra.utils.JVMStabilityInspector", true, me);
Class<?> killerIface  = Class.forName("org.apache.cassandra.utils.JVMKiller", true, me);
jvmStability.getMethod("replaceKiller", killerIface).invoke(null, new SpikeKiller(null));
```

`SpikeKiller.killJVM()` records the throwable and logs, and never exits.

### ClassLoader GC: **NO** — threads pin it

```
node1 loader collected: false
node2 loader collected: false
```

The spike instrumented *why*. Every thread below has its **thread class** loaded by the isolated
loader, so it is a GC root for that loader. All are daemon threads — they do not stop the JVM
exiting, but they do stop metaspace being reclaimed:

```
BackgroundTaskExecutor:1   GossipTasks:1            PendingRangeCalculator:1
IndexSummaryManager:1      SecondaryIndexManagement:1  HintsWriteExecutor:1
BatchlogTasks:1            SnapshotCleanup:1        NativePoolCleaner
nodes-info-persistence:1   JNA Cleaner              globalEventExecutor-2-N (non-daemon, transient)
```

This maps almost one-to-one onto shutdown calls that dtest's `Instance.shutdown()` makes and the
spike does not:

| surviving thread | missing call (from `Instance.java`) |
|---|---|
| `GossipTasks:1` | `Gossiper.instance.stopShutdownAndWait(1, MINUTES)` |
| `PendingRangeCalculator:1` | `PendingRangeCalculatorService.instance.shutdownAndWait(...)` |
| `IndexSummaryManager:1` | `IndexSummaryManager.instance.shutdownAndWait(...)` |
| `SecondaryIndexManagement:1` | `SecondaryIndexManager.shutdownAndWait(...)` |
| `BatchlogTasks:1` | `BatchlogManager.instance.shutdownAndWait(...)` |
| `HintsWriteExecutor:1` | `HintsService.instance.shutdownBlocking()` |
| `NativePoolCleaner` | `AbstractAllocatorMemtable.MEMORY_POOL.shutdownAndWait(...)` |
| `SnapshotCleanup:1` | `SnapshotManager.shutdownAndWait(...)` |
| `BackgroundTaskExecutor:1` | `CompactionManager.instance.forceShutdown()` |
| `globalEventExecutor-2-N` | `GlobalEventExecutor.INSTANCE.awaitInactivity(1, MINUTES)` |

Plus `PathUtils.runOnExitThreadsAndClear()`, `AuthCache.shutdownAllAndWait`, `Sampler`,
`StreamManager.instance.stop()`, `ActiveRepairService`, `PaxosRepair`, `UncommittedTableData`,
`DiagnosticSnapshotService`, `CompactionLogger`, `JMXBroadcastExecutor`, and
`DatabaseDescriptor.getCryptoProvider().uninstall()`.

**Two of the pinning threads have no clean fix available:**

* **`nodes-info-persistence:1`** — created in the fork-specific
  `org.apache.cassandra.nodes.Nodes` (`Nodes.java:206`,
  `executorFactory().sequential("nodes-info-persistence")`). `Nodes` exposes **no shutdown/close
  API at all**. This class does not exist upstream, so dtest's shutdown list does not cover it
  either. **The fork needs a `Nodes.shutdown()`** for loader GC to be achievable.
* **`JNA Cleaner`** — created by JNA itself (`com.sun.jna.internal.Cleaner`), one per classloader,
  never stopped by JNA. Inherent to loading JNA in a child loader.

**Practical consequence.** Metaspace from a stopped node is *not* reclaimed today. This is fine for
a co-hosted process that starts Cassandra once, and fine for a decommission test that ends with the
JVM. It is a real problem for anything that restarts the node in-place many times in one JVM (a
long test suite), which would leak a full Cassandra metaspace per restart. Closing that gap means
porting the full `Instance.shutdown()` list **and** adding `Nodes.shutdown()` to the fork.

---

## 11. Co-hosting with OpenSearch — concrete collision list

The OpenSearch container running on this box shows exactly the flags that would clash. Its real
command line (from `ps`) versus what Cassandra's `conf/` asks for:

### System properties (all JVM-global; only `cassandra.*` is namespaced and safe)

| property | Cassandra wants | OpenSearch sets | verdict |
|---|---|---|---|
| `io.netty.noUnsafe` | (unset → Unsafe used) | `true` | **HARD CONFLICT.** One JVM-wide value, read by both netty copies. Cassandra relies on Unsafe for off-heap buffers. |
| `io.netty.allocator.numDirectArenas` | (default) | `0` | **HARD CONFLICT** — disables Cassandra's direct arenas. |
| `io.netty.recycler.maxCapacityPerThread` | (default 4096) | `0` | **CONFLICT** (perf): disables netty object recycling for Cassandra. |
| `io.netty.noKeySetOptimization` | (unset) | `true` | **CONFLICT** (perf). |
| `io.netty.tryReflectionSetAccessible` | `true` | — | Cassandra sets it globally; affects OpenSearch's netty too. |
| `io.netty.allocator.useCacheForAllThreads`, `io.netty.allocator.maxOrder=11` | set | — | ditto. |
| `jna.nosys` | (unset) | `true` | **CONFLICT** — changes how Cassandra's JNA resolves native libs. |
| `java.library.path` | `$CASSANDRA_HOME/lib/sigar-bin` | its own | **single-valued** — only one wins. In the spike sigar failed to load (`no libsigar-amd64-linux.so in java.library.path`) and Cassandra degraded gracefully. |
| `java.security.manager` | `allow` | — | JVM-global. |
| `log4j2.disable.jmx` / `log4j.shutdownHookEnabled` | — | `true` / `false` | OpenSearch-only, harmless to Cassandra. |
| `java.io.tmpdir` | default | its own | single-valued; netty extracts natives here. |
| `jdk.attach.allowAttachSelf` | `true` (jamm) | `true` | compatible. |
| `--add-modules jdk.incubator.vector` | needed (jvector) | also sets it | compatible. |
| `cassandra.config`, `cassandra.storagedir`, `cassandra.logdir`, `cassandra.ring_delay_ms`, … | set | — | **safe** — `cassandra.*` namespaced. |
| **`cassandra-foreground`** | `yes` | — | **FLAG: not `cassandra.`-prefixed** (hyphen, not dot). JVM-global with a generic-looking name. |
| **`org.apache.cassandra.mbean_registration_class`** | `spike.impl.SpikeMBeanWrapper` | — | not `cassandra.*`-prefixed, but well namespaced. |
| **`java.rmi.server.hostname`**, `java.rmi.server.randomIDs`, `java.rmi.dgc.leaseValue`, `sun.rmi.transport.tcp.threadKeepAliveTime` | set by the JMX bring-up | — | **FLAG: JVM-global RMI tuning.** `java.rmi.server.hostname` is read **once** per JVM and cached; it also constrains multi-node JMX (§9). |
| `chronicle.analytics.disable`, `java.net.preferIPv4Stack` | from `jvm-server.options` | — | JVM-global. |

Exact set the spike installed (from the run log):

```
-Dcassandra.config=file:///.../conf/cassandra.yaml
-Dcassandra.storagedir=/.../storage
-Dcassandra-foreground=yes                      <-- NOT cassandra.-prefixed
-Dcassandra.logdir=/.../storage
-Dorg.apache.cassandra.mbean_registration_class=spike.impl.SpikeMBeanWrapper
-Dspike.node.name=cassandra-spike
-Dcassandra.ring_delay_ms=5000
-Dcassandra.consistent.rangemovement=false
-Dcassandra.superuser_setup_delay_ms=1000
-Dcassandra.skip_wait_for_gossip_to_settle=0
-Dcassandra.disable_tcactive_openssl=true
-Dio.netty.tryReflectionSetAccessible=true      <-- JVM-global, OpenSearch's netty sees it
-Djava.rmi.server.randomIDs=true                <-- JVM-global
-Djdk.attach.allowAttachSelf=true               <-- JVM-global
```

Plus, set inside `startJmx()`: `java.rmi.server.hostname`, `java.rmi.dgc.leaseValue=1000`,
`sun.rmi.transport.tcp.threadKeepAliveTime=1000` — **all JVM-global**.

**Recommendation:** the embedding host must own the JVM command line and reconcile the `io.netty.*`
and `jna.*` values once, up front. They cannot be set per-loader. Where the two products disagree
(`noUnsafe`, `numDirectArenas`, `recycler.maxCapacityPerThread`) somebody has to lose; measure
before choosing. Everything Cassandra reads through `CassandraRelevantProperties` is `cassandra.*`
and therefore safe.

### JNA

* Cassandra loads JNA for `mlockall`, and it **worked in both isolated loaders**:
  `INFO NativeLibrary -- JNA mlockall successful` (node1), and node2 reached the same call and
  failed only on `ENOMEM` (`Unable to lock JVM memory (ENOMEM) ... Increase RLIMIT_MEMLOCK`) —
  i.e. the native library loaded fine twice, the memory limit was the constraint.
* **No `UnsatisfiedLinkError: Native Library ... already loaded in another classloader`** anywhere.
  JNA extracts and loads `jnidispatch` per classloader.
* Cost: one **`JNA Cleaner` daemon thread per loader**, never stopped (see §10).
* Risk: `jna.nosys=true`, which OpenSearch sets, is JVM-global and changes resolution for
  Cassandra's JNA too.
* `mlockall` is a **process-wide** effect: whichever service calls it locks memory for the whole
  JVM, and the second caller sees `ENOMEM`. Only one of the two co-hosted services should do it.

### Netty

* The two netty copies are genuinely distinct classes:
  `netty is distinct across loaders: true`.
* The native transport loaded **twice, into two separate temp files**, proving netty's
  `NativeLibraryLoader` handles per-classloader loading correctly:

```
Successfully loaded the library /tmp/libnetty_transport_native_epoll_x86_646296043446479960065.so
Successfully loaded the library /tmp/libnetty_transport_native_epoll_x86_6415370046476309240943.so
```

  (`-Dio.netty.native.detectNativeLibraryDuplicates: true`, `deleteLibAfterLoading: true`.)
* Server netty is 4.1.135.Final in the child loader; the java driver's netty in the app loader is
  4.1.94.Final. They coexist without issue because they only meet over a socket.
* **The real netty risk is not classes, it is the `io.netty.*` system properties** — see the table.

### Logging: logback vs log4j2

* Cassandra's `logback-classic`/`logback-core` 1.5.35, `slf4j-api` 2.0.17, plus the
  `jcl-over-slf4j` and `log4j-over-slf4j` bridges, are all **private to the child loader**
  (`logback private to child: IsolatedClassLoader{cassandra, 116 urls}`), so SLF4J's
  service-loader binding resolves to logback inside the node and cannot see or be seen by
  OpenSearch's log4j2 in another loader.
* `shutdown()` stops the node's **own** `LoggerContext` only
  (`org.slf4j.LoggerFactory.getILoggerFactory().stop()` resolved through the child loader), so it
  cannot stop anyone else's logging.
* **Caveat 1:** `log4j-over-slf4j` in Cassandra's lib is a log4j **1.x** API shim. It does not
  collide with log4j **2.x**, but if anything ever puts real log4j2 and this shim in the *same*
  loader, expect confusion.
* **Caveat 2:** `-Dlogback.configurationFile` is JVM-global. The spike avoided it by pointing
  `cassandra.config`'s directory at a per-node `conf/` and letting `${cassandra.logdir}` (also
  JVM-global, but read at context-init time) drive `logback.xml`'s `<file>` paths. With two nodes
  this works only because they are started **sequentially**.
* The app-side driver printed `SLF4J(W): No SLF4J providers were found.` — expected and harmless;
  the application loader has `slf4j-api` but no binding, because the binding lives in the child.

### JCE / crypto provider

`DefaultCryptoProvider` (enabled by default in `cassandra.yaml`) installs a provider through
`java.security.Security.addProvider` and removes it via `Security.removeProvider`. That registry is
**JVM-global** and shared with OpenSearch's TLS stack. dtest explicitly calls
`DatabaseDescriptor.getCryptoProvider().uninstall()` during shutdown; the spike does not yet.
Either add ACCP to the classpath deliberately and accept the global install, or set
`crypto_provider: org.apache.cassandra.security.JREProvider`.

### Other JVM-global singletons worth naming

* **Shutdown hooks** — §10. Must be removed; per-service shutdown only.
* **`System.exit`** — §10. `runManaged=true` **and** `replaceKiller`.
* **Platform MBeanServer** — the node deliberately owns a private `MBeanServer`, so
  `org.apache.cassandra:*` MBeans never appear in the platform server that OpenSearch also uses.
  Only the *reads* are federated back to the platform server.
* **GC log / heap dump path / `-XX:CompileCommandFile`** — one value per JVM; `cassandra-env.sh`
  sets all three.
* **`Xlog:gc`** — a single destination; the two products cannot each have their own GC log.
* **RMI statics** — `java.rmi.server.hostname` cached once (§9).

---

## 12. Startup sequence that worked

```
1.  build IsolatedClassLoader(urls, shared=appLoader, parent=null)
       urls = [ target/classes, <maven tree of dse-db-all>, (+ lib/<arch>/*.jar) ]
2.  write a per-node conf/ (cassandra.yaml with listen/rpc/ports/seeds rewritten,
       logback.xml, cassandra-rackdc.properties, commitlog_archiving.properties,
       cassandra-jaas.config, hotspot_compiler)
3.  set the JVM-global system properties for THIS node (see §11)
4.  load spike.impl.CassandraNodeImpl through the child loader, cast to the shared
       bridge interface ICassandraNode
5.  Thread.currentThread().setContextClassLoader(child)      <-- required: Cassandra uses the
       TCCL for ExecutorFactory, service loading and logback context selection
6.  startJmx(bindAddr, port):
       - pin java.rmi.server.hostname / randomIDs / dgc.leaseValue / tcp.threadKeepAliveTime
       - force MBeanWrapper init -> SpikeMBeanWrapper -> private MBeanServer + federated view
       - RMIServerSocketFactoryImpl(addr) bound to loopback; client socket factory = null
       - JMXServerUtils$JmxRegistry(port, null, ssf, "jmxrmi")
       - RMIJRMPServerImpl + RMIConnectorServer(url, env, rmiServer, federatedMBeanServer)
       - registry.setRemoteServerStub(rmiServer.toStub())
7.  JVMStabilityInspector.replaceKiller(new SpikeKiller(...))  <-- BEFORE activate()
8.  new CassandraDaemon(true)      // runManaged=true => exitOrFail throws, never System.exit
9.  daemon.activate()
       applyConfig -> DatabaseDescriptor.daemonInitialization()  (reads cassandra.config)
       setup       -> commitlog, startup checks, schema, StorageService.initServer(), GCInspector
       start       -> native (CQL) transport
    NB: setup() also calls maybeInitJmx(); it no-ops because cassandra.jmx.{local,remote}.port
        are deliberately left UNSET and the embedder owns JMX.
10. JVMStabilityInspector.removeShutdownHooks()                <-- AFTER activate()
11. restore the previous TCCL
```

Ordering constraints that actually bit:

* `replaceKiller` must precede `activate()`; `removeShutdownHooks()` must follow it (the hook is
  registered inside `initServer()`).
* `startJmx` must precede `activate()`, because `GCInspector`'s constructor (run during `setup()`)
  queries `MBeanWrapper.instance` for `java.lang:type=GarbageCollector,*` — the federated wrapper
  has to exist by then or every GC throws (§7).
* `cassandra.jmx.local.port` / `cassandra.jmx.remote.port` must stay unset.
* With two nodes, steps 2–10 run to completion for node N before node N+1 starts.

---

## 13. Files

| path | what |
|---|---|
| `spike2/src/main/java/spike/bridge/IsolatedClassLoader.java` | the loader + shared predicate (§3) |
| `spike2/src/main/java/spike/bridge/ICassandraNode.java` | the only shared type |
| `spike2/src/main/java/spike/impl/CassandraNodeImpl.java` | start / startJmx / op / shutdown, inside the child loader |
| `spike2/src/main/java/spike/impl/SpikeMBeanWrapper.java` | private MBeanServer via `mbean_registration_class` |
| `spike2/src/main/java/spike/impl/FederatedMBeanServer.java` | private + platform MXBean federation (makes `nodetool info` work) |
| `spike2/src/main/java/spike/impl/SpikeKiller.java` | intercepts `System.exit(100)` |
| `spike2/src/main/java/spike/Main.java` | single-node driver + CQL round trip |
| `spike2/src/main/java/spike/TwoNodeDecom.java` | two nodes + real decommission + GC/pin diagnostics |
| `spike2/jvm21-server.options` | §6 |
| `spike2/spike-cassandra.in.sh` | JDK-21 fix for the stock scripts, used via `CASSANDRA_INCLUDE` |
| `run4.log`, `nodetool4.out` | single-node evidence |
| `run-2node-2.log`, `nodetool-2node.out` | two-node evidence |
| `run-decom.log`, `nodetool-after-decom.out` | decommission evidence |
| `run-exit.log` | `JVM-EXIT-CODE=0` |

---

## 14. What the real project must fix

1. `bin/cassandra.in.sh` — numeric version ladder; add `conf/jvm21-server.options` and
   `conf/jvm21-clients.options` (§5, §6).
2. Add `lib/$(uname -m)/*.jar` to the isolated classpath, or pin ACCP as a Maven dependency, or
   switch `crypto_provider` to `JREProvider` (§4).
3. Port the **full** `Instance.shutdown()` teardown list, plus
   `PathUtils.runOnExitThreadsAndClear()` and `cryptoProvider.uninstall()` (§10).
4. Add a `shutdown()` to the fork's `org.apache.cassandra.nodes.Nodes` — without it the isolated
   loader can never be collected (§10).
5. Reconcile `io.netty.*` and `jna.*` on one JVM command line before co-hosting (§11).
6. Decide the decommission test topology: 3 nodes with stock RFs, or 2 nodes + RF reduction (§9).
