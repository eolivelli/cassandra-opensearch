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
STOP_FAILURES=""
stop_node() {
    local n=$1
    echo "==> stopping node$n"
    if ! in_node "$n" cassandra-opensearch stop; then
        echo "    node$n did not stop cleanly" >&2
        STOP_FAILURES="$STOP_FAILURES $n"
    fi
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
