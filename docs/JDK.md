# JDK version: why 21

The original requirement was JDK 17. That turned out to be impossible to satisfy together with
"OpenSearch 3.5 or later in the same JVM", and the conflict is worth recording because it is not
a matter of configuration.

## The constraint

OpenSearch 3.x is compiled to **Java 21 bytecode**. Verified directly against the published
artifact rather than taken from documentation:

```
$ unzip -p opensearch-3.7.0.jar org/opensearch/.../ClusterStatsNodes\$JvmVersion.class | xxd | head -1
00000000: cafe babe 0000 0041 ...
                       ^^^^ 0x41 = 65 = Java 21
```

A JDK 17 JVM rejects class file major version 65 with `UnsupportedClassVersionError` at load
time. No compiler flag, classloader trick or `--release` setting changes this: it is the runtime
JVM refusing to parse the class file. Since the requirement is that both servers live in **one**
JVM, that JVM's version is bounded below by OpenSearch's 21.

The Cassandra side is bounded above. The fork declares:

```xml
<property name="java.supported" value="11,17,22"/>
```

and ships `conf/jvm11-server.options`, `conf/jvm17-server.options`, `conf/jvm22-server.options`
— with no entry for 21.

## The resolution

Runtime and build both use **JDK 21** (`21.0.11-tem`), chosen by the project owner from the
options presented.

Cassandra itself is consumed as **class-61 (Java 17) jars**: the fork's `build.xml` hard-fails
if ant runs on a JDK outside `java.supported`, so the fork is built with JDK 17 and installed to
the local repository as `com.datastax.dse:dse-db-all:5.0.7.0-SNAPSHOT`. Class-61 jars load
without issue on a JDK 21 JVM — the constraint only runs in the other direction.

```
build  Cassandra fork ──ant, JDK 17──> class 61 jars ─┐
                                                      ├──> one JVM, JDK 21
       OpenSearch     ──Maven Central──> class 65 jars ┘
       this project   ──maven, JDK 21 (release 21)────┘
```

`maven-enforcer-plugin` fails the build on any JDK other than 21 so this cannot drift silently.

## jvm21-server.options

Because the fork ships no `jvm21-server.options`, one is synthesized for the distribution. It is
derived from the 17/22 pair in the fork's `conf/` — see `dist/src/main/resources/conf/` — and the
flags it contains were validated by actually starting a node on 21, not by reading release notes.

## Rebuilding the Cassandra dependency

```bash
cd ~/dev/cassandra
JAVA_HOME=~/.sdkman/candidates/java/17.0.19-tem ant mvn-install
```

This installs `com.datastax.dse:dse-db-all` and `com.datastax.db:db-all` at `5.0.7.0-SNAPSHOT`
into `~/.m2`. It writes only into the fork's `build/` directory and touches no sources.
