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
#   destroy           stop everything and delete the cluster directory
#
set -euo pipefail

CLUSTER_DIR="$(pwd)/cluster"

while getopts ":d:h" opt; do
    case "$opt" in
        d) CLUSTER_DIR=$OPTARG ;;
        h) sed -n '2,15p' "$0" >&2; exit 2 ;;
        *) sed -n '2,15p' "$0" >&2; exit 2 ;;
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

start_node() {
    local n=$1
    echo "==> starting node$n ($(node_host "$n"))"
    in_node "$n" cassandra-opensearch start -d
}

stop_node() {
    local n=$1
    echo "==> stopping node$n"
    in_node "$n" cassandra-opensearch stop || true
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
        echo "Cluster is up. Check it with: $0 -d $CLUSTER_DIR ring"
        ;;

    stop)
        if [ -n "$NODE" ]; then
            stop_node "$NODE"
        else
            for n in $(all_nodes | tac); do
                stop_node "$n"
            done
        fi
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
        # --force is required below three nodes: system_distributed and system_auth are RF 3, so
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
        for n in $(all_nodes | tac); do
            stop_node "$n"
        done
        echo "==> removing $CLUSTER_DIR"
        rm -rf "$CLUSTER_DIR"
        ;;

    *)
        sed -n '2,15p' "$0" >&2
        exit 2
        ;;
esac
