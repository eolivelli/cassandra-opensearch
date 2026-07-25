#!/usr/bin/env bash
#
# Creates an N-node cassandra-opensearch cluster on this machine, one process per node.
#
# Each node is a complete, independent installation of the distribution. That is deliberate and
# not merely convenient: a node is a whole JVM hosting both servers, so "several nodes on one
# machine" means several processes, exactly as it would across several machines. Nothing here is
# a test harness shortcut.
#
# Usage:  ./create-cluster.sh [-n NODES] [-d DIRECTORY] [-t TARBALL]
#
set -euo pipefail

NODES=3
CLUSTER_DIR="$(pwd)/cluster"
TARBALL=""

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [-n NODES] [-d DIRECTORY] [-t TARBALL]

  -n NODES      number of nodes to create (default 3, max 9)
  -d DIRECTORY  where to create them (default ./cluster)
  -t TARBALL    path to cassandra-opensearch-*-bin.tar.gz
                (default: found under ../../dist/tarball/target)
EOF
    exit 2
}

while getopts ":n:d:t:h" opt; do
    case "$opt" in
        n) NODES=$OPTARG ;;
        d) CLUSTER_DIR=$OPTARG ;;
        t) TARBALL=$OPTARG ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Nine is not an arbitrary limit: node i binds 127.0.0.$i, and 127.0.0.10 upwards would still
# work but the port arithmetic below stops being readable. Nine nodes on one machine is already
# well past what the memory of a developer laptop enjoys.
if ! [[ "$NODES" =~ ^[1-9]$ ]]; then
    echo "error: -n must be between 1 and 9 (got '$NODES')" >&2
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "$TARBALL" ]; then
    TARBALL=$(ls "$SCRIPT_DIR"/../../dist/tarball/target/cassandra-opensearch-*-bin.tar.gz 2>/dev/null | head -1 || true)
fi
if [ ! -f "$TARBALL" ]; then
    echo "error: no tarball found. Build one with 'mvn -o clean install' from the project root," >&2
    echo "       or pass one with -t." >&2
    exit 1
fi

if [ -e "$CLUSTER_DIR" ]; then
    echo "error: $CLUSTER_DIR already exists. Remove it or choose another -d." >&2
    exit 1
fi

# --- port plan ------------------------------------------------------------------------------
#
# Node i listens on 127.0.0.$i, which Linux routes to lo with no alias configuration needed.
# macOS does not: see the README.
#
# The ports are also distinct per node, even though the addresses already differ. Differing
# addresses alone would satisfy Cassandra — that is what ccm does — but the JDK management agent,
# Cassandra's JMX connector and OpenSearch's transport each bind on their own terms, and one of
# them choosing the wildcard address turns a shared port into a cross-node collision that
# presents as "a node just failed to start".
#
# Nothing uses a stock port (9042, 9200, 7000, 7199). A developer machine very often already has
# something there, and if that something is a container published on 0.0.0.0 then every loopback
# alias is taken with it — moving to 127.0.0.2 does not help.
port() { # port <node> <offset>
    echo $((21000 + $1 * 10 + $2))
}
storage_port()       { port "$1" 0; }
ssl_storage_port()   { port "$1" 1; }
native_port()        { port "$1" 2; }
cassandra_jmx_port() { port "$1" 3; }
supervisor_jmx_port() { port "$1" 4; }
http_port()          { port "$1" 5; }
transport_port()     { port "$1" 6; }
host()               { echo "127.0.0.$1"; }

# --- build the cluster ----------------------------------------------------------------------

mkdir -p "$CLUSTER_DIR"
echo "Creating a $NODES-node cluster in $CLUSTER_DIR"
echo "  from $(basename "$TARBALL")"
echo

# OpenSearch needs the transport address of every node before any of them boots, and it must be
# addresses rather than node names: a node's name is the Cassandra host id it is assigned at
# runtime, which nothing knows until the process is up.
seed_hosts=""
for i in $(seq 1 "$NODES"); do
    [ -n "$seed_hosts" ] && seed_hosts="$seed_hosts, "
    seed_hosts="$seed_hosts\"$(host "$i"):$(transport_port "$i")\""
done
bootstrap_nodes="\"$(host 1):$(transport_port 1)\""
cassandra_seed="$(host 1):$(storage_port 1)"

for i in $(seq 1 "$NODES"); do
    NODE_DIR="$CLUSTER_DIR/node$i"
    mkdir -p "$NODE_DIR"
    tar xzf "$TARBALL" -C "$NODE_DIR" --strip-components=1

    H=$(host "$i")
    CONF="$NODE_DIR/conf"

    # Cassandra parses conf/cassandra.yaml itself, so its ports and addresses live there.
    # The identically-named keys in cassandra-opensearch.yaml reach nothing (docs/KNOWN-GAPS.md
    # section 2) — which is exactly why both files must be edited, and edited to agree.
    sed -i \
        -e "s|^storage_port: .*|storage_port: $(storage_port "$i")|" \
        -e "s|^ssl_storage_port: .*|ssl_storage_port: $(ssl_storage_port "$i")|" \
        -e "s|^native_transport_port: .*|native_transport_port: $(native_port "$i")|" \
        -e "s|^listen_address: .*|listen_address: $H|" \
        -e "s|^rpc_address: .*|rpc_address: $H|" \
        -e "s|^\( *- seeds:\).*|\1 \"$cassandra_seed\"|" \
        "$CONF/cassandra.yaml"

    sed -i \
        -e "s|^\( *\)listen_address: .*|\1listen_address: $H|" \
        -e "s|^\( *\)storage_port: .*|\1storage_port: $(storage_port "$i")|" \
        -e "s|^\( *\)native_transport_port: .*|\1native_transport_port: $(native_port "$i")|" \
        -e "s|^\( *\)jmx_port: .*|\1jmx_port: $(cassandra_jmx_port "$i")\n\1jmx_address: $H|" \
        -e "s|^\( *\)network_host: .*|\1network_host: $H|" \
        -e "s|^\( *\)http_port: .*|\1http_port: $(http_port "$i")|" \
        -e "s|^\( *\)transport_port: .*|\1transport_port: $(transport_port "$i")|" \
        "$CONF/cassandra-opensearch.yaml"

    # The shipped opensearch.yml uses single-node discovery, where a node elects itself and never
    # forms a cluster with anybody. A real cluster has to replace it outright.
    if [ "$NODES" -gt 1 ]; then
        sed -i \
            -e "s|^discovery.type: .*|discovery.seed_hosts: [$seed_hosts]\ncluster.initial_cluster_manager_nodes: [$bootstrap_nodes]|" \
            -e "s|^network.host: .*|network.host: $H|" \
            "$CONF/opensearch.yml"
    else
        sed -i -e "s|^network.host: .*|network.host: $H|" "$CONF/opensearch.yml"
    fi

    # The launcher reads these; they are what bin/cassandra-opensearch status|stop|decommission
    # use to find this node's supervisor MBean.
    cat > "$NODE_DIR/node.env" <<EOF
# Sourced by cluster-control.sh. One node's identity in one place.
export CASSANDRA_OPENSEARCH_JMX_HOST=$H
export CASSANDRA_OPENSEARCH_JMX_PORT=$(supervisor_jmx_port "$i")
export MAX_HEAP_SIZE=\${MAX_HEAP_SIZE:-1g}
export MAX_DIRECT_MEMORY_SIZE=\${MAX_DIRECT_MEMORY_SIZE:-1g}
EOF

    printf 'node%d  %-9s  cql %s  rest %s  nodetool-jmx %s  supervisor-jmx %s\n' \
        "$i" "$H" "$(native_port "$i")" "$(http_port "$i")" \
        "$(cassandra_jmx_port "$i")" "$(supervisor_jmx_port "$i")"
done

cat > "$CLUSTER_DIR/cluster.env" <<EOF
CLUSTER_NODES=$NODES
EOF

echo
echo "Created. Start it with:"
echo "  $SCRIPT_DIR/cluster-control.sh -d $CLUSTER_DIR start"
