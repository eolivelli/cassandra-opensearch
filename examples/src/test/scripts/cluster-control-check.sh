#!/usr/bin/env bash
#
# Drives local-cluster/cluster-control.sh against a cluster of stub nodes.
#
# The scripts under local-cluster/ are the operator's entry point, and until now the only thing
# the build checked about them was that they parse. That is not enough for the two things this
# file exists for, both of which were live defects with `bash -n` passing:
#
#   - `destroy` refusing to delete a cluster that was already stopped, because the launcher
#     reports exit code 3 (the CLI's NOT_RUNNING) for a node that is down and the script counted
#     any non-zero exit as "did not stop cleanly". That is the ordinary case - a freshly created
#     cluster, one the operator just stopped, the one the README's worked example leaves behind -
#     and create-cluster.sh refuses to reuse the directory, so the only way out was rm -rf.
#   - `destroy -f` doing nothing, because -f was consumed by getopts before the command word and
#     so only `-f destroy` worked, while the script's own help and its refusal message both
#     document `destroy -f`.
#
# A stub node is enough to test both: what is under test is how the script reads exit codes, and
# a real node would only make that slower and less deterministic.
#
# usage: cluster-control-check.sh <local-cluster-directory> <work-directory>

set -eu

SCRIPTS="${1:?usage: cluster-control-check.sh <local-cluster-directory> <work-directory>}"
WORK="${2:?usage: cluster-control-check.sh <local-cluster-directory> <work-directory>}"

CONTROL="$SCRIPTS/cluster-control.sh"
CREATE="$SCRIPTS/create-cluster.sh"
[ -x "$CONTROL" ] || { echo "cluster-control-check: no such script: $CONTROL" >&2; exit 1; }

FAILURES=0

fail() { echo "  FAIL  $*" >&2; FAILURES=$((FAILURES + 1)); }
pass() { echo "  ok    $*"; }

# A cluster directory whose nodes are stubs: `bin/cassandra-opensearch <command>` exits with
# whatever STUB_STOP_RC says for `stop`, and 0 for anything else.
make_cluster() { # make_cluster <dir> <nodes> <stop-rc>
    local dir=$1 nodes=$2 stop_rc=$3 n
    rm -rf "$dir"
    mkdir -p "$dir"
    echo "CLUSTER_NODES=$nodes" > "$dir/cluster.env"
    for n in $(seq 1 "$nodes"); do
        mkdir -p "$dir/node$n/bin"
        : > "$dir/node$n/node.env"
        cat > "$dir/node$n/bin/cassandra-opensearch" <<EOF
#!/bin/sh
# Stub node. Only the exit code matters to cluster-control.sh.
case "\$1" in
    stop) echo "stub node: nothing is running" >&2; exit $stop_rc ;;
    *) exit 0 ;;
esac
EOF
        chmod +x "$dir/node$n/bin/cassandra-opensearch"
    done
}

# --- destroy on a cluster that is already stopped ---------------------------------------------
#
# Exit code 3 is the launcher's, propagated from the CLI: NOT_RUNNING. It is the answer `stop`
# gives for a node that is down, and it is not a failure.

CLUSTER="$WORK/already-stopped"
make_cluster "$CLUSTER" 2 3
RC=0
OUT=$("$CONTROL" -d "$CLUSTER" destroy 2>&1) || RC=$?
if [ "$RC" -ne 0 ]; then
    fail "destroy on an already-stopped cluster exited $RC: $OUT"
elif [ -d "$CLUSTER" ]; then
    fail "destroy left $CLUSTER behind"
else
    pass "destroy deletes a cluster whose nodes were already stopped"
fi

# --- destroy still refuses when a node really did fail to stop ---------------------------------

CLUSTER="$WORK/stop-failed"
make_cluster "$CLUSTER" 2 1
RC=0
OUT=$("$CONTROL" -d "$CLUSTER" destroy 2>&1) || RC=$?
if [ "$RC" -eq 0 ]; then
    fail "destroy deleted a cluster with a node that would not stop"
elif [ ! -d "$CLUSTER" ]; then
    fail "destroy deleted $CLUSTER despite a node that would not stop"
elif ! echo "$OUT" | grep -q 'refusing to delete'; then
    fail "destroy did not explain why it refused: $OUT"
else
    pass "destroy still refuses a node that genuinely would not stop"
fi

# --- -f after the command word, which is the documented spelling -------------------------------

RC=0
OUT=$("$CONTROL" -d "$CLUSTER" destroy -f 2>&1) || RC=$?
if [ "$RC" -ne 0 ]; then
    fail "\`destroy -f\` exited $RC: $OUT"
elif [ -d "$CLUSTER" ]; then
    fail "\`destroy -f\` left $CLUSTER behind; -f was swallowed by getopts before the command"
else
    pass "\`destroy -f\` deletes a cluster a node would not release"
fi

# --- and before it, which is what used to be the only working form -----------------------------

CLUSTER="$WORK/stop-failed-flag-first"
make_cluster "$CLUSTER" 1 1
RC=0
OUT=$("$CONTROL" -d "$CLUSTER" -f destroy 2>&1) || RC=$?
if [ "$RC" -eq 0 ] && [ ! -d "$CLUSTER" ]; then
    pass "\`-f destroy\` still works"
else
    fail "\`-f destroy\` exited $RC and left the directory: $OUT"
fi

# --- stop over a cluster that is already down --------------------------------------------------

CLUSTER="$WORK/stop-when-down"
make_cluster "$CLUSTER" 3 3
RC=0
OUT=$("$CONTROL" -d "$CLUSTER" stop 2>&1) || RC=$?
if [ "$RC" -ne 0 ]; then
    fail "stop on an already-stopped cluster exited $RC: $OUT"
elif ! echo "$OUT" | grep -q 'was not running'; then
    fail "stop did not say the nodes were already down: $OUT"
else
    pass "stop is not a failure when every node is already down"
fi
rm -rf "$CLUSTER"

# --- an unknown option is still an error -------------------------------------------------------

CLUSTER="$WORK/unknown-option"
make_cluster "$CLUSTER" 1 0
RC=0
OUT=$("$CONTROL" -d "$CLUSTER" destroy --wat 2>&1) || RC=$?
if [ "$RC" -eq 2 ] && [ -d "$CLUSTER" ]; then
    pass "an unknown option is usage, not a silently ignored word"
else
    fail "\`destroy --wat\` exited $RC and may have deleted the cluster: $OUT"
fi
rm -rf "$CLUSTER"

# --- create-cluster.sh's rewrite() -------------------------------------------------------------
#
# rewrite() is a library function inside create-cluster.sh, so it is exercised the way the shell
# allows: by sourcing the script with a flag that makes it stop before it does anything. It has
# no such flag, so instead the function is lifted out and run on its own - the same text, checked
# to still be there.

if [ -r "$CREATE" ]; then
    REWRITE_DIR="$WORK/rewrite"
    rm -rf "$REWRITE_DIR"
    mkdir -p "$REWRITE_DIR"

    # The function, with everything around it left behind.
    sed -n '/^rewrite() {/,/^}/p' "$CREATE" > "$REWRITE_DIR/rewrite.sh"
    if [ ! -s "$REWRITE_DIR/rewrite.sh" ]; then
        fail "create-cluster.sh has no rewrite() to check"
    else
        printf 'a\nb\n' > "$REWRITE_DIR/conf.yaml"
        chmod 640 "$REWRITE_DIR/conf.yaml"
        ln -sf conf.yaml "$REWRITE_DIR/link.yaml"

        # umask 077 is the operator this used to punish: a fresh temp file is created under it,
        # and mv put that mode on the conf file.
        (
            set -eu
            umask 077
            # shellcheck disable=SC1090
            . "$REWRITE_DIR/rewrite.sh"
            rewrite "$REWRITE_DIR/conf.yaml" sed -e 's/^a$/A/'
            rewrite "$REWRITE_DIR/link.yaml" sed -e 's/^b$/B/'
            # A command that fails must leave nothing behind. Its complaint is expected, and is
            # dropped so that it does not read as a build failure in the log.
            rewrite "$REWRITE_DIR/conf.yaml" sh -c 'exit 7' 2> /dev/null || true
        )

        MODE=$(ls -l "$REWRITE_DIR/conf.yaml" | cut -c1-10)
        if [ "$MODE" = "-rw-r-----" ]; then
            pass "rewrite() keeps the file's mode under a restrictive umask"
        else
            fail "rewrite() changed the mode of conf.yaml to $MODE (expected -rw-r-----)"
        fi

        if [ -L "$REWRITE_DIR/link.yaml" ]; then
            pass "rewrite() edits through a symlink rather than replacing it"
        else
            fail "rewrite() replaced the symlink link.yaml with a regular file"
        fi

        if grep -q '^A$' "$REWRITE_DIR/conf.yaml" && grep -q '^B$' "$REWRITE_DIR/conf.yaml"; then
            pass "rewrite() applied both edits to the same underlying file"
        else
            fail "rewrite() did not apply its edits: $(cat "$REWRITE_DIR/conf.yaml")"
        fi

        LEFTOVERS=$(find "$REWRITE_DIR" -name '*.rewrite.*' | wc -l | tr -d ' ')
        if [ "$LEFTOVERS" -eq 0 ]; then
            pass "rewrite() cleans up its temp file when the command fails"
        else
            fail "rewrite() left $LEFTOVERS temp file(s) behind after a failing command"
        fi
    fi
fi

echo
if [ "$FAILURES" -eq 0 ]; then
    echo "example scripts OK"
    exit 0
fi
echo "$FAILURES example-script check(s) failed" >&2
exit 1
