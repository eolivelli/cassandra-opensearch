#!/usr/bin/env bash
#
# Starts, stops and inspects a cluster created by create-cluster.sh.
#
# Usage:  ./cluster-control.sh [-d DIRECTORY] <command> [node]
#
#   start [n]         start all nodes in order, or just node n
#   stop [n]          stop all nodes in reverse order, or just node n
#   status [n]        supervisor status for all nodes, or just node n
#   ring              nodetool status, from node 1
#   health            OpenSearch cluster health and node list, from node 1
#   decommission n    retire node n through the coupled procedure
#   destroy [-f]      stop everything and delete the cluster directory
#
# Options:
#   -d DIRECTORY      the cluster directory (default ./cluster)
#   -f                destroy: delete even if a node could not be stopped
#
# Options may be written before or after the command, so `destroy -f` and `-f destroy` are the
# same thing. A node that was already stopped is not a failure and does not need -f.
#
set -euo pipefail

CLUSTER_DIR="$(pwd)/cluster"
FORCE=no

# The comment block above, printed from line 2 up to the first line that is not a comment. A fixed
# line range goes stale the moment a line is added to it, which is how `set -euo pipefail` used to
# end up in the help output.
usage() {
    awk 'NR == 1 { next } /^#/ { print; next } { exit }' "$0" >&2
}

while getopts ":d:fh" opt; do
    case "$opt" in
        d) CLUSTER_DIR=$OPTARG ;;
        f) FORCE=yes ;;
        h) usage; exit 2 ;;
        *) usage; exit 2 ;;
    esac
done
shift $((OPTIND - 1))

# A second pass over what is left. getopts stops at the first non-option word, so every flag
# written after the command - `destroy -f`, which is the form the help above and the refusal
# message below both document - was collected as a positional argument and silently dropped, and
# -f did nothing at all unless it was written before the command as `-f destroy`.
POSITIONAL=""
while [ $# -gt 0 ]; do
    case "$1" in
        -f|--force) FORCE=yes ;;
        -d|--directory)
            shift
            [ $# -gt 0 ] || { echo "error: -d needs a directory" >&2; exit 2; }
            CLUSTER_DIR=$1
            ;;
        -h|--help) usage; exit 2 ;;
        -*) echo "error: unknown option '$1'" >&2; usage; exit 2 ;;
        # No path or node number this script accepts contains whitespace - node numbers are 1..9 -
        # so a space-separated list is enough and keeps this a plain POSIX-shaped loop.
        *) POSITIONAL="$POSITIONAL $1" ;;
    esac
    shift
done
# shellcheck disable=SC2086  # deliberate word splitting; see above
set -- $POSITIONAL

COMMAND="${1:-}"
NODE="${2:-}"

if [ ! -f "$CLUSTER_DIR/cluster.env" ]; then
    echo "error: no cluster in $CLUSTER_DIR. Run create-cluster.sh first, or pass -d." >&2
    exit 1
fi
# shellcheck disable=SC1091
source "$CLUSTER_DIR/cluster.env"

node_dir()  { echo "$CLUSTER_DIR/node$1"; }
node_host() { echo "127.0.0.$1"; }
http_port() { echo $((21000 + $1 * 10 + 5)); }

# Runs a node's own bin/ script with that node's identity in the environment.
in_node() { # in_node <n> <script> [args...]
    local n=$1; shift
    local dir; dir=$(node_dir "$n")
    # shellcheck disable=SC1091
    ( source "$dir/node.env"; "$dir/bin/$@" )
}

all_nodes() { seq 1 "$CLUSTER_NODES"; }
# Reverse order, for shutdown. `seq <n> -1 1` and not `| tac`: macOS has no tac, and these scripts
# are documented as working there.
all_nodes_reverse() { seq "$CLUSTER_NODES" -1 1; }

start_node() {
    local n=$1
    echo "==> starting node$n ($(node_host "$n"))"
    in_node "$n" cassandra-opensearch start -d
}

# Records rather than swallows a failure. `destroy` deletes the data directory of every node it
# stopped, and doing that to a node that is still running is how a JVM ends up writing sstables
# into a directory that no longer exists.
#
# Exit code 3 is NOT a failure. It is the CLI's NOT_RUNNING - nothing was listening on the
# supervisor's JMX port - which is the ordinary state of a node that has already been stopped, or
# that was never started. The launcher propagates it verbatim. Reading it as "did not stop
# cleanly" made `destroy` refuse to delete a cluster that was entirely down, which is the exact
# case the README's worked example leaves behind, and create-cluster.sh then refuses to reuse the
# directory - so the operator's only way out was rm -rf by hand.
STOP_FAILURES=""
stop_node() {
    local n=$1
    local rc=0
    echo "==> stopping node$n"
    in_node "$n" cassandra-opensearch stop || rc=$?
    case "$rc" in
        0) ;;
        3) echo "    node$n was not running" ;;
        *)
            echo "    node$n did not stop cleanly (exit $rc)" >&2
            STOP_FAILURES="$STOP_FAILURES $n"
            ;;
    esac
}

case "$COMMAND" in
    start)
        if [ -n "$NODE" ]; then
            start_node "$NODE"
        else
            # Strictly in order. Node 1 is the seed for both clusters: Cassandra reads
            # seed_provider at daemon initialisation and OpenSearch resolves
            # cluster.initial_cluster_manager_nodes while the Node is being constructed, so a
            # later node that cannot reach node 1 within its discovery timeout will bootstrap a
            # second cluster of its own rather than join this one.
            for n in $(all_nodes); do
                start_node "$n"
            done
        fi
        echo
        echo "Cluster is up. Check it with: $0 -d '$CLUSTER_DIR' ring"
        ;;

    stop)
        if [ -n "$NODE" ]; then
            stop_node "$NODE"
        else
            for n in $(all_nodes_reverse); do
                stop_node "$n"
            done
        fi
        [ -z "$STOP_FAILURES" ] || { echo "error: node(s)$STOP_FAILURES did not stop" >&2; exit 1; }
        ;;

    status)
        for n in ${NODE:-$(all_nodes)}; do
            echo "==> node$n"
            in_node "$n" cassandra-opensearch status || true
            echo
        done
        ;;

    ring)
        in_node 1 nodetool status
        ;;

    health)
        host=$(node_host 1); port=$(http_port 1)
        echo "==> cluster health"
        curl -s "http://$host:$port/_cluster/health?pretty"
        echo "==> nodes"
        curl -s "http://$host:$port/_cat/nodes?v"
        ;;

    decommission)
        [ -z "$NODE" ] && { echo "error: decommission needs a node number" >&2; exit 2; }
        # --force is required below four nodes: system_distributed and system_auth are RF 3, so
        # Cassandra refuses an unforced decommission that would drop the ring under the
        # replication factor. At four or more nodes the unforced path works.
        force=""
        if [ "$CLUSTER_NODES" -lt 4 ]; then
            echo "note: fewer than 4 nodes, so --force is needed (system_distributed is RF 3)"
            force="--force"
        fi
        echo "==> decommissioning node$NODE"
        in_node "$NODE" cassandra-opensearch decommission $force
        ;;

    destroy)
        for n in $(all_nodes_reverse); do
            stop_node "$n"
        done
        # A node that would not stop is still running, with open file handles into the directory
        # about to be deleted. Deleting it anyway leaves a JVM writing sstables and OpenSearch
        # translog into unlinked inodes, holding its ports until somebody finds and kills it.
        if [ -n "$STOP_FAILURES" ] && [ "$FORCE" = no ]; then
            echo "error: node(s)$STOP_FAILURES did not stop; refusing to delete $CLUSTER_DIR" >&2
            echo "       Stop them by hand, or re-run with -f to delete anyway." >&2
            exit 1
        fi
        echo "==> removing $CLUSTER_DIR"
        rm -rf "$CLUSTER_DIR"
        ;;

    *)
        usage
        exit 2
        ;;
esac
