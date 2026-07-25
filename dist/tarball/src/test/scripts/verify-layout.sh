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
SKIPS=0

fail() {
    echo "  FAIL  $*" >&2
    FAILURES=$((FAILURES + 1))
}

pass() {
    echo "  ok    $*"
}

# For a check that needs a tool this machine does not have. Counted and reported at the end
# rather than passed silently: a check that quietly does nothing is worse than no check.
skip() {
    echo "  skip  $*" >&2
    SKIPS=$((SKIPS + 1))
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

# unzip is not part of a JDK-only machine's tool set, and without this guard its absence looked
# exactly like a malformed manifest: `unzip -p` not being there prints nothing on stdout, and the
# grep below then failed for a reason the message did not mention. The JDK's own jar tool is the
# fallback, since this build requires a JDK 21 to have run at all.
MANIFEST_TOOL=none
if command -v unzip > /dev/null 2>&1; then
    MANIFEST_TOOL=unzip
elif [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/jar" ]; then
    MANIFEST_TOOL=jar
fi

print_manifest() { # print_manifest <jar>
    case "$MANIFEST_TOOL" in
        unzip) unzip -p "$1" META-INF/MANIFEST.MF 2> /dev/null ;;
        jar)
            rm -rf "$WORK/manifest-check"
            mkdir -p "$WORK/manifest-check"
            (cd "$WORK/manifest-check" && "$JAVA_HOME/bin/jar" xf "$1" META-INF/MANIFEST.MF) \
                > /dev/null 2>&1 || return 0
            cat "$WORK/manifest-check/META-INF/MANIFEST.MF" 2> /dev/null
            ;;
    esac
}

for service in cassandra opensearch; do
    jar="$HOME_DIR/lib/$service/000-conf-classpath.jar"
    if [ ! -f "$jar" ]; then
        fail "lib/$service has no 000-conf-classpath.jar; the child loader will not see conf/"
        continue
    fi
    if [ "$MANIFEST_TOOL" = none ]; then
        skip "lib/$service conf/ classpath: neither unzip nor \$JAVA_HOME/bin/jar is available"
        continue
    fi
    if print_manifest "$jar" | tr -d '\r' | grep -q '^Class-Path: \.\./\.\./conf/$'; then
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
    #
    # The "accepts" half asserts a POSITIVE signal, not merely the absence of "requires Java 21":
    # absence is also what a launcher that failed for some completely different reason produces,
    # so the old form scored every other breakage as a pass. Port 1 is never listening, so a
    # launcher that got all the way through to running the CLI jar exits 3 - the CLI's documented
    # "nothing is running" - and says so. Nothing else in this script reaches that far.
    if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
        AMBIENT_JAVA=$("${JAVA_HOME}/bin/java" -version 2>&1 \
            | awk -F'"' '/version/ { split($2, v, /[.-]/); print v[1]; exit }')
        STATUS_RC=0
        STATUS_OUT=$(CASSANDRA_OPENSEARCH_JMX_PORT=1 "$HOME_DIR/bin/cassandra-opensearch" status 2>&1) \
            || STATUS_RC=$?
        if [ "$AMBIENT_JAVA" = 21 ]; then
            if [ "$STATUS_RC" -eq 3 ] \
                    && echo "$STATUS_OUT" | grep -q 'Cannot reach a cassandra-opensearch process'; then
                pass "bin/cassandra-opensearch accepts the JDK 21 in JAVA_HOME and runs the CLI"
            else
                fail "bin/cassandra-opensearch did not reach the CLI on a JDK 21 (exit $STATUS_RC): $STATUS_OUT"
            fi
        else
            if echo "$STATUS_OUT" | grep -q 'requires Java 21'; then
                pass "bin/cassandra-opensearch rejects the JDK $AMBIENT_JAVA in JAVA_HOME"
            else
                fail "bin/cassandra-opensearch did not reject the JDK $AMBIENT_JAVA (exit $STATUS_RC): $STATUS_OUT"
            fi
        fi
    fi

    # A conf/ directory without jvm21-server.options must stop the launcher, loudly, BEFORE it
    # starts a JVM. The guard used to live in a function called from inside a command
    # substitution, where its `exit 1` ended only the subshell: the launcher printed the error and
    # then started the JVM anyway, with no --add-exports and no --add-opens, which dies inside
    # startup with "IllegalAccessError: module java.rmi does not export sun.rmi.registry".
    #
    # The Dockerfile's own advice - mount a conf/ directory to change the bind addresses - is how
    # an operator arrives here.
    #
    # `start -d` and not a foreground start: -d never execs, so a regression fails this check
    # instead of hanging the build on a JVM that will not exit.
    if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
        EMPTY_CONF="$WORK/conf-without-options"
        rm -rf "$EMPTY_CONF"
        mkdir -p "$EMPTY_CONF"
        GUARD_PID="$WORK/should-never-be-written.pid"
        rm -f "$GUARD_PID"
        GUARD_RC=0
        GUARD_OUT=$(CASSANDRA_OPENSEARCH_CONF="$EMPTY_CONF" \
            CASSANDRA_OPENSEARCH_JMX_PORT=1 \
            "$HOME_DIR/bin/cassandra-opensearch" start -d -p "$GUARD_PID" 2>&1) || GUARD_RC=$?

        if [ "$GUARD_RC" -eq 0 ]; then
            fail "bin/cassandra-opensearch start exited 0 with no conf/jvm21-server.options"
        elif ! echo "$GUARD_OUT" | grep -q 'jvm21-server.options'; then
            fail "a missing conf/jvm21-server.options is not reported by name: $GUARD_OUT"
        elif echo "$GUARD_OUT" | grep -q 'started pid'; then
            fail "bin/cassandra-opensearch started a JVM despite the missing conf/jvm21-server.options"
        elif [ -f "$GUARD_PID" ]; then
            fail "bin/cassandra-opensearch wrote $GUARD_PID despite the missing conf/jvm21-server.options"
        else
            pass "bin/cassandra-opensearch refuses to start without conf/jvm21-server.options"
        fi

        # Same for the tools: an empty conf/ has to stop nodetool before the JVM, not after.
        NODETOOL_RC=0
        NODETOOL_OUT=$(CASSANDRA_OPENSEARCH_CONF="$EMPTY_CONF" \
            "$HOME_DIR/bin/nodetool" version 2>&1) || NODETOOL_RC=$?
        if [ "$NODETOOL_RC" -ne 0 ] && echo "$NODETOOL_OUT" | grep -q 'jvm21-server.options'; then
            pass "bin/nodetool refuses to run without conf/jvm21-server.options"
        else
            fail "bin/nodetool did not report the missing conf/jvm21-server.options (exit $NODETOOL_RC): $NODETOOL_OUT"
        fi

        # The tools must not drag jdk.incubator.vector in with them: it is there for jvector and
        # Lucene inside the server, and every JDK that loads an incubator module warns about it
        # first - on every single nodetool invocation an operator makes.
        if "$HOME_DIR/bin/nodetool" version 2>&1 | grep -q 'Using incubator modules'; then
            fail "bin/nodetool prints the JDK's incubator-module warning"
        else
            pass "bin/nodetool does not enable incubator modules"
        fi
    fi
fi

echo
if [ "$SKIPS" -ne 0 ]; then
    echo "$SKIPS check(s) skipped for missing tools" >&2
fi
if [ "$FAILURES" -eq 0 ]; then
    echo "distribution layout OK"
    exit 0
fi
echo "$FAILURES layout check(s) failed" >&2
exit 1
