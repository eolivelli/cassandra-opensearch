#!/bin/sh
#
# Sourced by every script in bin/. It answers four questions the same way for all of them:
# where is the installation, which java, is that java usable, and what does it need on its
# command line.
#
# NOT usable from an operator's own script. Two things tie it to this bin/: the installation root
# is derived from "$0", which names the sourcing script rather than this file, and co_die calls
# exit, which would take the sourcing shell with it. Call bin/cassandra-opensearch instead.

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
#
# Returns non-zero rather than calling co_die, because every caller uses it inside a command
# substitution: `exit` there ends the subshell the substitution runs in and nothing else, so the
# script would sail on and start a JVM with no --add-exports at all - which dies deep inside
# startup with "IllegalAccessError: module java.rmi does not export sun.rmi.registry". The caller
# must therefore test the status; see set_java_arguments in bin/cassandra-opensearch.
co_jvm_options() {
    _co_options_file="$CO_CONF/jvm21-server.options"
    if [ ! -r "$_co_options_file" ]; then
        echo "cassandra-opensearch: missing $_co_options_file" >&2
        echo "  Every JVM this distribution starts needs its --add-exports and --add-opens" >&2
        echo "  entries. Set CASSANDRA_OPENSEARCH_CONF, or restore the file - a mounted conf/" >&2
        echo "  directory has to carry all of conf/, not just the files you meant to change." >&2
        return 1
    fi
    _co_options=$(grep '^-' "$_co_options_file" | tr '\n' ' ')
    case "$_co_options" in
        *[!\ ]*) ;;
        *)
            echo "cassandra-opensearch: $_co_options_file has no JVM options in it." >&2
            echo "  Expected lines beginning with '-'; without them the JVM starts without the" >&2
            echo "  module opens both servers need and fails with IllegalAccessError." >&2
            return 1
            ;;
    esac
    printf '%s' "$_co_options"
}
