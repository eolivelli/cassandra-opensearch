#!/bin/sh
#
# Sourced by every script in bin/. It answers four questions the same way for all of them:
# where is the installation, which java, is that java usable, and what does it need on its
# command line.
#
# Nothing here execs anything, so it is also safe to source from an operator's own script.

# --- installation root ------------------------------------------------------------------

# Follows symlinks, so `ln -s .../bin/cassandra-opensearch /usr/local/bin/` still finds conf/
# and lib/ rather than looking for them in /usr/local.
co_resolve_bin_dir() {
    _co_script="$1"
    while [ -h "$_co_script" ]; do
        _co_dir=$(cd -P "$(dirname "$_co_script")" > /dev/null && pwd)
        _co_link=$(ls -ld "$_co_script" | sed -e 's/.*-> //')
        case "$_co_link" in
            /*) _co_script="$_co_link" ;;
            *)  _co_script="$_co_dir/$_co_link" ;;
        esac
    done
    cd -P "$(dirname "$_co_script")" > /dev/null && pwd
}

CO_BIN=$(co_resolve_bin_dir "$0")
CO_HOME=$(cd -P "$CO_BIN/.." > /dev/null && pwd)
CO_CONF="${CASSANDRA_OPENSEARCH_CONF:-$CO_HOME/conf}"
CO_LOGS="${CASSANDRA_OPENSEARCH_LOGS:-$CO_HOME/logs}"

# The supervisor's own JMX connector - `status`, `stop` and `decommission` talk to it. This is
# NOT Cassandra's JMX port: that one belongs to the embedded node and serves a federated MBean
# server whose non-platform half is private to the child ClassLoader, so the supervisor's MBean
# is not reachable through it. bin/nodetool uses that one; everything else uses this one.
CO_JMX_HOST="${CASSANDRA_OPENSEARCH_JMX_HOST:-127.0.0.1}"
CO_JMX_PORT="${CASSANDRA_OPENSEARCH_JMX_PORT:-7299}"

co_die() {
    echo "cassandra-opensearch: $*" >&2
    exit 1
}

# --- java -------------------------------------------------------------------------------

if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME/bin/java"
    [ -x "$JAVA" ] || co_die "JAVA_HOME is set to $JAVA_HOME but $JAVA is not executable."
else
    JAVA=$(command -v java 2> /dev/null)
    [ -n "$JAVA" ] || co_die "no java on PATH and JAVA_HOME is not set."
fi

# Prints the JDK's feature version as a plain integer: 21, 17, 11. Handles both the modern
# "21.0.11" and the legacy "1.8.0_402" shapes, and drops any -ea / -LTS suffix.
co_java_major() {
    _co_version=$("${1:-$JAVA}" -version 2>&1 | awk -F'"' '/version/ { print $2; exit }')
    _co_version=${_co_version%%-*}
    case "$_co_version" in
        1.*) _co_version=${_co_version#1.} ;;
    esac
    echo "${_co_version%%.*}"
}

# The whole stack is a JDK 21 stack, in both directions: OpenSearch 3.x jars are class file
# major 65 and will not load below 21, and Cassandra's ThreadAwareSecurityManager needs a
# SecurityManager, which JDK 24 removed. Failing here beats failing inside the JVM with a
# ClassFormatError or an UnsupportedOperationException from a thread nobody is watching.
co_require_java_21() {
    _co_major=$(co_java_major "$JAVA")
    if [ "$_co_major" != "21" ]; then
        co_die "this distribution requires Java 21, but $JAVA is Java ${_co_major:-unknown}.
  OpenSearch 3.x jars are Java 21 bytecode and cannot load on anything older; JDK 24 and later
  removed the SecurityManager that Cassandra's UDF sandbox installs at startup.
  Set JAVA_HOME to a JDK 21 installation."
    fi
}

# The contents of conf/jvm21-server.options, ready to be expanded UNQUOTED by the caller.
#
# Unquoted is not sloppiness: `--add-exports java.base/sun.nio.ch=ALL-UNNAMED` has to arrive at
# the JVM as two argv entries, and word splitting is what produces them. Passing each line as a
# single argument gives "Unrecognized option: --add-exports java.base/sun.nio.ch=ALL-UNNAMED".
co_jvm_options() {
    _co_options_file="$CO_CONF/jvm21-server.options"
    [ -r "$_co_options_file" ] || co_die "missing $_co_options_file"
    grep '^-' "$_co_options_file" | tr '\n' ' '
}
