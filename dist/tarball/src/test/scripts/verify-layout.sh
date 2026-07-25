#!/bin/sh
#
# Unpacks the tarball and checks it against docs/ARCHITECTURE.md.
#
# The assembly running to completion proves nothing about the thing that actually matters: that
# lib/boot holds no server jar and neither child directory holds the SPI. Both of those are
# silent at build time and expensive at runtime - a Cassandra class on the system classpath is
# loaded by the parent and shared with both isolated worlds, which is exactly the coupling the
# ClassLoader isolation exists to prevent, and it would not fail until something in OpenSearch
# resolved the wrong Netty.
#
# usage: verify-layout.sh <tarball> <work-directory>

set -eu

ARCHIVE="${1:?usage: verify-layout.sh <tarball> <work-directory>}"
WORK="${2:?usage: verify-layout.sh <tarball> <work-directory>}"

FAILURES=0

fail() {
    echo "  FAIL  $*" >&2
    FAILURES=$((FAILURES + 1))
}

pass() {
    echo "  ok    $*"
}

check() {
    if [ "$1" = 0 ]; then shift; pass "$@"; else shift; fail "$@"; fi
}

[ -f "$ARCHIVE" ] || { echo "verify-layout: no such archive: $ARCHIVE" >&2; exit 1; }

rm -rf "$WORK"
mkdir -p "$WORK"
tar -xzf "$ARCHIVE" -C "$WORK"

HOME_DIR=$(find "$WORK" -mindepth 1 -maxdepth 1 -type d | head -1)
[ -n "$HOME_DIR" ] || { echo "verify-layout: the archive has no top-level directory" >&2; exit 1; }
echo "layout of $(basename "$ARCHIVE") -> $(basename "$HOME_DIR")"

# --- directories ---------------------------------------------------------------------------

for directory in bin conf lib/boot lib/cassandra lib/opensearch data logs; do
    if [ -d "$HOME_DIR/$directory" ]; then pass "$directory/"; else fail "$directory/ is missing"; fi
done

count_jars() {
    find "$HOME_DIR/$1" -maxdepth 1 -name '*.jar' | wc -l | tr -d ' '
}

jars_matching() {
    find "$HOME_DIR/$1" -maxdepth 1 -name "$2" | wc -l | tr -d ' '
}

BOOT_JARS=$(count_jars lib/boot)
CASSANDRA_JARS=$(count_jars lib/cassandra)
OPENSEARCH_JARS=$(count_jars lib/opensearch)
echo "jars: boot=$BOOT_JARS cassandra=$CASSANDRA_JARS opensearch=$OPENSEARCH_JARS"

# --- lib/boot ------------------------------------------------------------------------------
#
# The JVM's -cp, and therefore the one classpath both isolated loaders can see through their
# parent. Everything here is a deliberate decision; the list is short enough to enumerate.

EXPECTED_BOOT="cassandra-opensearch-bootstrap cassandra-opensearch-cli cassandra-opensearch-server cassandra-opensearch-spi slf4j-api slf4j-simple snakeyaml"
for artifact in $EXPECTED_BOOT; do
    if [ "$(jars_matching lib/boot "$artifact-*.jar")" -ge 1 ]; then
        pass "lib/boot has $artifact"
    else
        fail "lib/boot is missing $artifact"
    fi
done

UNEXPECTED_BOOT=$(find "$HOME_DIR/lib/boot" -maxdepth 1 -name '*.jar' -exec basename {} \; \
    | while read -r jar; do
        matched=no
        for artifact in $EXPECTED_BOOT; do
            case "$jar" in "$artifact"-*.jar) matched=yes ;; esac
        done
        if [ "$matched" = no ]; then echo "$jar"; fi
    done)
if [ -z "$UNEXPECTED_BOOT" ]; then
    pass "lib/boot holds nothing else"
else
    fail "lib/boot holds jars that are not part of the supervisor: $(echo "$UNEXPECTED_BOOT" | tr '\n' ' ')"
fi

# The specific catastrophes, named so the failure explains itself.
for forbidden in 'dse-db-all*.jar' 'opensearch-[0-9]*.jar' 'lucene-*.jar' 'netty-*.jar' \
                 'logback-*.jar' 'log4j-*.jar' 'cassandra-opensearch-runtime-*.jar'; do
    if [ "$(jars_matching lib/boot "$forbidden")" -eq 0 ]; then
        pass "lib/boot has no $forbidden"
    else
        fail "lib/boot has $forbidden - it would be loaded by the parent and forced on both children"
    fi
done

# --- lib/cassandra -------------------------------------------------------------------------

check "$([ "$(jars_matching lib/cassandra 'cassandra-opensearch-spi-*.jar')" -eq 0 ] && echo 0 || echo 1)" \
    "lib/cassandra does not shadow the SPI"
check "$([ "$(jars_matching lib/cassandra 'cassandra-opensearch-runtime-cassandra-*.jar')" -eq 1 ] && echo 0 || echo 1)" \
    "lib/cassandra has runtime-cassandra"
check "$([ "$(jars_matching lib/cassandra 'dse-db-all-*.jar')" -eq 1 ] && echo 0 || echo 1)" \
    "lib/cassandra has dse-db-all"
check "$([ "$CASSANDRA_JARS" -ge 110 ] && echo 0 || echo 1)" \
    "lib/cassandra holds the whole tree ($CASSANDRA_JARS jars, expected >= 110)"
check "$([ "$(jars_matching lib/cassandra 'logback-classic-*.jar')" -ge 1 ] && echo 0 || echo 1)" \
    "lib/cassandra has logback-classic"

# slf4j 1.7 here would be silent and total: logback 1.5 is discovered through slf4j 2's
# ServiceLoader and ships no 1.7-era StaticLoggerBinder, so the node would run on the NOP
# logger and write no log at all.
SLF4J_JAR=$(find "$HOME_DIR/lib/cassandra" -maxdepth 1 -name 'slf4j-api-*.jar' -exec basename {} \; | head -1)
case "$SLF4J_JAR" in
    slf4j-api-2.*) pass "lib/cassandra resolves slf4j 2.x ($SLF4J_JAR)" ;;
    "") fail "lib/cassandra has no slf4j-api" ;;
    *) fail "lib/cassandra has $SLF4J_JAR; logback 1.5 needs slf4j 2.x or the node runs on the NOP logger" ;;
esac

# --- lib/opensearch ------------------------------------------------------------------------

check "$([ "$(jars_matching lib/opensearch 'cassandra-opensearch-spi-*.jar')" -eq 0 ] && echo 0 || echo 1)" \
    "lib/opensearch does not shadow the SPI"
check "$([ "$(jars_matching lib/opensearch 'cassandra-opensearch-runtime-opensearch-*.jar')" -eq 1 ] && echo 0 || echo 1)" \
    "lib/opensearch has runtime-opensearch"
check "$([ "$(jars_matching lib/opensearch 'opensearch-[0-9]*.jar')" -eq 1 ] && echo 0 || echo 1)" \
    "lib/opensearch has the opensearch server jar"
check "$([ "$(jars_matching lib/opensearch 'log4j-core-*.jar')" -eq 1 ] && echo 0 || echo 1)" \
    "lib/opensearch has log4j-core (optional in the OpenSearch POM; Node.<init> needs it)"
check "$([ "$OPENSEARCH_JARS" -ge 60 ] && echo 0 || echo 1)" \
    "lib/opensearch holds the whole closure ($OPENSEARCH_JARS jars, expected >= 60)"
check "$([ "$(jars_matching lib/opensearch 'jna-*.jar')" -eq 0 ] && echo 0 || echo 1)" \
    "lib/opensearch excludes jna"
check "$([ "$(jars_matching lib/opensearch 'netty-codec-native-quic-*.jar')" -eq 0 ] && echo 0 || echo 1)" \
    "lib/opensearch excludes netty-codec-native-quic"

# Neither child may see the other's world.
check "$([ "$(jars_matching lib/opensearch 'dse-db-all-*.jar')" -eq 0 ] && echo 0 || echo 1)" \
    "lib/opensearch has no Cassandra"
check "$([ "$(jars_matching lib/cassandra 'opensearch-[0-9]*.jar')" -eq 0 ] && echo 0 || echo 1)" \
    "lib/cassandra has no OpenSearch"

# --- conf/ on each child's classpath ---------------------------------------------------------

for service in cassandra opensearch; do
    jar="$HOME_DIR/lib/$service/000-conf-classpath.jar"
    if [ ! -f "$jar" ]; then
        fail "lib/$service has no 000-conf-classpath.jar; the child loader will not see conf/"
        continue
    fi
    if unzip -p "$jar" META-INF/MANIFEST.MF 2> /dev/null | tr -d '\r' | grep -q '^Class-Path: \.\./\.\./conf/$'; then
        pass "lib/$service puts conf/ on the child classpath"
    else
        fail "lib/$service/000-conf-classpath.jar does not carry Class-Path: ../../conf/"
    fi
done

# --- conf ------------------------------------------------------------------------------------

for file in cassandra-opensearch.yaml cassandra.yaml opensearch.yml jvm21-server.options \
            logback.xml logback-tools.xml log4j2.properties; do
    if [ -f "$HOME_DIR/conf/$file" ]; then pass "conf/$file"; else fail "conf/$file is missing"; fi
done

# Without this the node never forms a cluster and start() fails on the health timeout.
if grep -q '^discovery.type: single-node' "$HOME_DIR/conf/opensearch.yml"; then
    pass "conf/opensearch.yml sets discovery.type: single-node"
else
    fail "conf/opensearch.yml does not set discovery.type: single-node"
fi

# %node_name throws on every log event until LogConfigurator.setNodeName is called, which this
# project never does.
if grep -v '^[[:space:]]*#' "$HOME_DIR/conf/log4j2.properties" | grep -q '%node_name'; then
    fail "conf/log4j2.properties uses %node_name, which throws unless LogConfigurator.setNodeName is called"
else
    pass "conf/log4j2.properties avoids %node_name"
fi

# The default provider loads AmazonCorrettoCryptoProvider, which is in neither the Maven tree nor
# this layout.
if grep -q 'org.apache.cassandra.security.JREProvider' "$HOME_DIR/conf/cassandra.yaml"; then
    pass "conf/cassandra.yaml uses the JRE crypto provider"
else
    fail "conf/cassandra.yaml does not set crypto_provider to JREProvider"
fi

# jvm17-server.options carries this; the package no longer exists on 21 and it warns on every start.
if grep '^-' "$HOME_DIR/conf/jvm21-server.options" | grep -q 'jdk.internal.util.jar'; then
    fail "conf/jvm21-server.options opens jdk.internal.util.jar, which does not exist on JDK 21"
else
    pass "conf/jvm21-server.options has no JDK 17-only entries"
fi

# --- bin -------------------------------------------------------------------------------------

for script in cassandra-opensearch cassandra-opensearch.in.sh cassandra.in.sh nodetool cqlsh \
              opensearch-plugin opensearch-keystore; do
    path="$HOME_DIR/bin/$script"
    if [ ! -f "$path" ]; then
        fail "bin/$script is missing"
        continue
    fi
    if [ ! -x "$path" ]; then
        fail "bin/$script is not executable"
        continue
    fi
    if sh -n "$path" 2> /dev/null; then
        pass "bin/$script"
    else
        fail "bin/$script is not valid POSIX shell"
    fi
done

# --- the launcher's own checks ----------------------------------------------------------------

if [ -x "$HOME_DIR/bin/cassandra-opensearch" ]; then
    if JAVA_HOME=/nonexistent-jdk "$HOME_DIR/bin/cassandra-opensearch" status 2>&1 \
            | grep -q 'is not executable'; then
        pass "bin/cassandra-opensearch reports an unusable JAVA_HOME"
    else
        fail "bin/cassandra-opensearch does not report an unusable JAVA_HOME clearly"
    fi

    # The JDK check runs before anything else in every script, and getting it wrong is how a
    # JDK 17 or a JDK 25 gets to fail later, somewhere much less legible. Which way round this
    # assertion goes depends on what JAVA_HOME actually is, so both directions get exercised
    # depending on where the build runs.
    if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
        AMBIENT_JAVA=$("${JAVA_HOME}/bin/java" -version 2>&1 \
            | awk -F'"' '/version/ { split($2, v, /[.-]/); print v[1]; exit }')
        if "$HOME_DIR/bin/cassandra-opensearch" status 2>&1 | grep -q 'requires Java 21'; then
            REJECTED=yes
        else
            REJECTED=no
        fi
        if [ "$AMBIENT_JAVA" = 21 ] && [ "$REJECTED" = no ]; then
            pass "bin/cassandra-opensearch accepts the JDK 21 in JAVA_HOME"
        elif [ "$AMBIENT_JAVA" != 21 ] && [ "$REJECTED" = yes ]; then
            pass "bin/cassandra-opensearch rejects the JDK $AMBIENT_JAVA in JAVA_HOME"
        else
            fail "bin/cassandra-opensearch got the JDK check wrong for Java $AMBIENT_JAVA (rejected=$REJECTED)"
        fi
    fi
fi

echo
if [ "$FAILURES" -eq 0 ]; then
    echo "distribution layout OK"
    exit 0
fi
echo "$FAILURES layout check(s) failed" >&2
exit 1
