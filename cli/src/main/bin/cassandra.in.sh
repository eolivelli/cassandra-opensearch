#!/bin/sh
#
# The CASSANDRA_INCLUDE shim.
#
# Every tool in the Cassandra fork's bin/ - nodetool, cqlsh, sstable* - starts by sourcing an
# include file, and honours CASSANDRA_INCLUDE in preference to its own conf/cassandra.in.sh.
# This file is what we point that at, and it exists for two reasons.
#
# THE VERSION CHECK. The fork's cassandra.in.sh compares JVM versions as STRINGS:
#
#     elif [ "$JVM_VERSION" \< "22" ] ; then echo "DSE DB 5.0 requires Java 11 or higher."; exit 1
#
# "21.0.11" < "22" is true, so every JDK from 18 to 21 is rejected with a message about needing
# Java 11 - taking nodetool, cqlsh and every sstable tool with it. The ladder below is the same
# decision made numerically. This is a bug the fork should fix upstream; until it does, we work
# around it from the outside, because the fork is not ours to modify.
#
# THE LAYOUT. This distribution is not a Cassandra installation: its jars are in
# lib/cassandra/ rather than lib/, there is no lib/jamm-0.4.0.jar for the -javaagent the fork's
# include adds unconditionally, and there is no lib/sigar-bin. Replacing the include rather than
# patching it means the tools get a classpath that actually exists.

# Set by bin/nodetool and friends before sourcing this, so that a tool run through
# CASSANDRA_INCLUDE from anywhere still finds the installation.
if [ -z "$CO_HOME" ]; then
    echo "cassandra.in.sh: CO_HOME is not set. This file is meant to be used as the" >&2
    echo "CASSANDRA_INCLUDE of this distribution's bin/ wrappers, not on its own." >&2
    exit 1
fi

CASSANDRA_HOME="$CO_HOME"
CASSANDRA_CONF="${CASSANDRA_OPENSEARCH_CONF:-$CO_HOME/conf}"

# conf/ first: that is where logback-tools.xml and cassandra.yaml are found.
CLASSPATH="$CASSANDRA_CONF"
for jar in "$CO_HOME"/lib/cassandra/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

# Where the node keeps its data, so that the offline sstable tools look in the right place.
# Must agree with `directories.data` in conf/cassandra-opensearch.yaml.
cassandra_storagedir="${CASSANDRA_OPENSEARCH_DATA:-$CO_HOME/data}/cassandra"

if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA=$(command -v java 2> /dev/null)
fi
if [ -z "$JAVA" ] || [ ! -x "$JAVA" ]; then
    echo "cassandra.in.sh: no java found. Set JAVA_HOME." >&2
    exit 1
fi

# The numeric ladder the fork's string comparison was trying to be. JAVA_VERSION names the
# newest options profile the JDK is compatible with, which is what the fork's scripts use it
# for; on JDK 21 that is the 17 profile.
jvm_feature_version=$("$JAVA" -version 2>&1 | awk -F'"' '/version/ { print $2; exit }')
jvm_feature_version=${jvm_feature_version%%-*}
case "$jvm_feature_version" in
    1.*) jvm_feature_version=${jvm_feature_version#1.} ;;
esac
jvm_feature_version=${jvm_feature_version%%.*}

if [ "$jvm_feature_version" -lt 11 ] 2> /dev/null; then
    echo "Cassandra requires Java 11 or higher; found $jvm_feature_version." >&2
    exit 1
elif [ "$jvm_feature_version" -lt 17 ]; then
    JAVA_VERSION=11
elif [ "$jvm_feature_version" -lt 22 ]; then
    JAVA_VERSION=17
else
    JAVA_VERSION=22
fi
JVM_VERSION="$jvm_feature_version"

# Only the module-system and system-property entries from the server options: the tools run in a
# 128 MiB heap, where the G1 region and young-generation tuning meant for a server either does
# not apply or prints a warning. The --add-exports and --add-opens entries do apply - nodetool
# reaches sun.rmi.registry through the same code the node does.
#
# Left unset on purpose: JAVA_AGENT. The fork's include always adds
# -javaagent:$CASSANDRA_HOME/lib/jamm-0.4.0.jar, which is not part of this layout and would make
# every tool fail to start.
#
# The missing-file case is fatal and has to be checked before the loop, not inside it: `for opt in
# $(grep ...)` over a file that is not there iterates zero times and leaves JVM_OPTS empty, and
# nodetool then dies with "IllegalAccessError: module java.rmi does not export sun.rmi.registry"
# a long way from the cause. A conf/ directory mounted over the shipped one is how that happens.
if [ ! -r "$CASSANDRA_CONF/jvm21-server.options" ]; then
    echo "cassandra.in.sh: missing $CASSANDRA_CONF/jvm21-server.options." >&2
    echo "  The Cassandra tools need its --add-exports and --add-opens entries; without them" >&2
    echo "  nodetool fails with IllegalAccessError on sun.rmi.registry. Restore the file or set" >&2
    echo "  CASSANDRA_OPENSEARCH_CONF to a directory that has it." >&2
    exit 1
fi

# Dropped for the tools only: --add-modules jdk.incubator.vector is there for jvector and Lucene
# inside the server, and no tool touches either - but every JDK that loads an incubator module
# prints "WARNING: Using incubator modules" first, on every single nodetool invocation.
JVM_OPTS_FROM_FILE=$(grep -E '^(--|-D)' "$CASSANDRA_CONF/jvm21-server.options" \
    | grep -v '^--add-modules[ =]*jdk.incubator.vector' || true)
if [ -z "$JVM_OPTS_FROM_FILE" ]; then
    echo "cassandra.in.sh: $CASSANDRA_CONF/jvm21-server.options carries no --add-exports," >&2
    echo "  --add-opens or -D entries. nodetool cannot run without them." >&2
    exit 1
fi
for opt in $JVM_OPTS_FROM_FILE; do
    JVM_OPTS="$JVM_OPTS $opt"
done

export CASSANDRA_HOME CASSANDRA_CONF CLASSPATH JAVA JAVA_VERSION JVM_VERSION JVM_OPTS cassandra_storagedir
