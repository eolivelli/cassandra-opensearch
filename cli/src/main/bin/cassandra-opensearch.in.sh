#!/bin/sh
#
# Sourced by every script in bin/. It answers four questions the same way for all of them:
# where is the installation, which java, is that java usable, and what does it need on its
# command line.
#
# NOT usable from an operator's own script. Two things tie it to this bin/: the installation root
# is derived from "$0", which names the sourcing script rather than this file, and co_die calls
# exit, which would take the sourcing shell with it. Call bin/cassandra-opensearch instead.

# Defined first, because everything below it - starting with the very first assignment - may need
# to fail with a message rather than with `set -e`'s silence.
co_die() {
    echo "cassandra-opensearch: $*" >&2
    exit 1
}

# --- installation root ------------------------------------------------------------------

# Follows symlinks, so a script reached through `ln -s .../bin/cassandra-opensearch
# /usr/local/bin/` finds conf/ and lib/ in the installation rather than in /usr/local.
#
# This function cannot be what makes that symlink work on its own, and it never could: it lives in
# the file the sourcing script has to have found already. bin/cassandra-opensearch repeats the
# resolution inline, before it looks for this include, which is what makes the symlink above
# usable. The other wrappers in bin/ - nodetool, cqlsh, opensearch-plugin, opensearch-keystore -
# deliberately do not: they take `dirname "$0"` at face value and must be invoked through their
# real path. Symlink bin/cassandra-opensearch, or put bin/ on PATH.
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

# Both take the exit status of their command substitution, and both are read under `set -e`, so
# each needs an explicit failure branch or the sourcing script dies silently on a bin/ that has
# been moved out from under it.
CO_BIN=$(co_resolve_bin_dir "$0") \
    || co_die "cannot resolve the bin/ directory this script was run from ($0)."
CO_HOME=$(cd -P "$CO_BIN/.." > /dev/null && pwd) \
    || co_die "cannot resolve the installation root above $CO_BIN."
CO_CONF="${CASSANDRA_OPENSEARCH_CONF:-$CO_HOME/conf}"
CO_LOGS="${CASSANDRA_OPENSEARCH_LOGS:-$CO_HOME/logs}"

# The supervisor's own JMX connector - `status`, `stop` and `decommission` talk to it. This is
# NOT Cassandra's JMX port: that one belongs to the embedded node and serves a federated MBean
# server whose non-platform half is private to the child ClassLoader, so the supervisor's MBean
# is not reachable through it. bin/nodetool uses that one; everything else uses this one.
CO_JMX_HOST="${CASSANDRA_OPENSEARCH_JMX_HOST:-127.0.0.1}"
CO_JMX_PORT="${CASSANDRA_OPENSEARCH_JMX_PORT:-7299}"

# --- processes ---------------------------------------------------------------------------

# Whether a pid names a process that is still there.
#
# `kill -0` alone is not that question. It answers EPERM, not ESRCH, for a live process owned by
# another uid - a node started by root or by a service account, which is the ordinary case in an
# installed deployment and in any container that drops privileges. Reading that as "not running"
# is what let `start -d` launch a second JVM onto a data directory another JVM already had open.
#
# `ps -p` answers regardless of ownership, so it is the second opinion - and only ever a positive
# one. `ps -p` failing does NOT mean the process is gone: busybox ps takes no -p at all (it is an
# XSI option, not base POSIX), so on a stripped container image the command fails for a reason that
# has nothing to do with the pid, and reading that as "not running" inverts the whole function on
# exactly the images it was written for. Its failure is therefore inconclusive and falls through.
#
# The last resort is kill's own message: ESRCH is the only failure that means the process is gone,
# so anything else - EPERM, a ps that does not exist, a ps that does not understand -p - is read as
# "still there". Every fallback errs towards "alive", because refusing to start is recoverable and
# starting twice is not.
co_process_is_alive() { # co_process_is_alive <pid>
    if kill -0 "$1" 2> /dev/null; then
        return 0
    fi
    if command -v ps > /dev/null 2>&1 && ps -p "$1" > /dev/null 2>&1; then
        return 0
    fi
    case "$(kill -0 "$1" 2>&1 || true)" in
        *'o such process'*) return 1 ;;
        *) return 0 ;;
    esac
}

# --- java -------------------------------------------------------------------------------

if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME/bin/java"
    [ -x "$JAVA" ] || co_die "JAVA_HOME is set to $JAVA_HOME but $JAVA is not executable."
else
    # `|| true` is load-bearing, and this file is sourced by scripts that all run under `set -e`:
    # an assignment whose value is a command substitution takes that substitution's exit status,
    # so on a machine with no java the sourcing script died at rc=127 with no output at all and
    # the co_die below was unreachable. Same shape as the one in bin/cqlsh.
    JAVA=$(command -v java 2> /dev/null || true)
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
