# Cassandra + OpenSearch in one JVM.
#
# The build context is the tarball module's target/ directory, so the archive the assembly just
# produced is the only thing shipped to the daemon. `mvn verify -Pdocker` wires it up.
#
# eclipse-temurin:21-jre and not a JDK image: nothing in the running distribution compiles, and
# not 22 or later - Cassandra's ThreadAwareSecurityManager installs a SecurityManager, which JDK
# 24 removed. bin/cassandra-opensearch checks the version at startup and refuses anything else,
# so a base image bump that crosses that line fails loudly rather than at 3 a.m.

FROM eclipse-temurin:21-jre

ARG DIST_ARCHIVE
ARG DIST_NAME

LABEL org.opencontainers.image.title="cassandra-opensearch" \
      org.opencontainers.image.description="An Apache Cassandra node and an OpenSearch node in one JVM, isolated by ClassLoader" \
      org.opencontainers.image.source="https://github.com/eolivelli/cassandra-opensearch"

# procps for `ps` and `top`. Nothing in bin/ uses them - the launcher's daemon mode tests liveness
# with `kill -0`, which is a shell builtin - so this is purely so that `docker exec ... ps` works
# for an operator looking at a container that is misbehaving. Kept to one layer and cleaned in the
# same step.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends procps; \
    rm -rf /var/lib/apt/lists/*

# Non-root. Cassandra's data directory and OpenSearch's shards are written by this user, so the
# uid is fixed rather than assigned: a bind-mounted data directory has to be chownable to
# something predictable, and 1000 is what a host user usually is.
#
# The base image is Ubuntu-derived and already ships a placeholder account at 1000, which is why
# the ids are freed first - groupadd exits 4 on a duplicate gid rather than reusing it.
RUN set -eux; \
    if getent passwd 1000 > /dev/null; then userdel --remove "$(getent passwd 1000 | cut -d: -f1)"; fi; \
    if getent group 1000 > /dev/null; then groupdel "$(getent group 1000 | cut -d: -f1)"; fi; \
    groupadd --gid 1000 cassandra-opensearch; \
    useradd --uid 1000 --gid 1000 --no-create-home --shell /sbin/nologin cassandra-opensearch

ENV CASSANDRA_OPENSEARCH_HOME=/opt/cassandra-opensearch
ENV PATH="$CASSANDRA_OPENSEARCH_HOME/bin:$PATH"

# ADD unpacks a local tarball. --chown does the ownership in the same layer that writes the files:
# a later `chown -R` rewrites the metadata of every file, and a layer diff has no way to express
# that except by copying all ~130 MB of the distribution a second time.
#
# The version-free path is a symlink for the same reason - `mv` moves the whole tree into a second
# layer, which costs another full copy. The launcher resolves its installation root with `cd -P`,
# so CASSANDRA_OPENSEARCH_HOME lands on the versioned directory the symlink points at, and both
# spellings name the same files.
ADD --chown=1000:1000 ${DIST_ARCHIVE} /opt/
RUN set -eux; \
    ln -s "/opt/${DIST_NAME}" "$CASSANDRA_OPENSEARCH_HOME"; \
    mkdir -p "/opt/${DIST_NAME}/data" "/opt/${DIST_NAME}/logs"; \
    chown 1000:1000 "/opt/${DIST_NAME}/data" "/opt/${DIST_NAME}/logs"

USER 1000:1000
WORKDIR $CASSANDRA_OPENSEARCH_HOME

# data/ and logs/ are the only paths written at run time; everything else is read-only.
VOLUME ["/opt/cassandra-opensearch/data", "/opt/cassandra-opensearch/logs"]

# 9042 CQL, 7000 internode, 7199 the node's JMX (nodetool), 9200 the OpenSearch REST API,
# 9300 OpenSearch transport.
#
# Publishing these is not on its own enough to reach them from outside the container. The
# shipped conf/cassandra.yaml binds listen_address and rpc_address to 127.0.0.1 and
# conf/opensearch.yml binds network.host the same way, which is the right default for a
# single-node process and the wrong one for a published port. Mount a conf/ directory with the
# addresses you want - Cassandra additionally needs broadcast_address and broadcast_rpc_address
# set whenever the bind address is 0.0.0.0.
#
# The supervisor's JMX port (7299) is deliberately not published: the connector is
# unauthenticated and bound to loopback, so anything that talks to it has to run inside the
# container. Publishing it means putting a password file behind it first.
#
#   docker exec <container> cassandra-opensearch status         what the supervisor reports
#   docker exec <container> cassandra-opensearch decommission   leave both clusters, then exit
#   docker stop <container>                                     ordered shutdown
#
# `docker stop`, and not `docker exec ... stop`: the CMD below runs the node in the foreground as
# pid 1, and SIGTERM is what the supervisor's shutdown hook is waiting for. `docker exec ...
# stop` does reach the same shutdown - the foreground launcher writes the pid file too, so it
# finds pid 1 and signals it - but the exec is itself a process inside the container, so it is
# killed along with everything else the moment pid 1 exits and reports 137 for a shutdown that
# actually succeeded. `docker stop` sends the same signal and waits for the container.
#
# Give it room: `docker stop` sends SIGKILL after 10 seconds by default, and the ordered shutdown
# takes about 8 on an idle single-node process - longer with data to drain. Use
# `docker run --stop-timeout` or `docker stop -t`, because a SIGKILLed Cassandra replays its
# commit log on the next start.
EXPOSE 7000 7199 9042 9200 9300

# The defaults the launcher would otherwise pick, named here so `docker run -e` can change them.
ENV MAX_HEAP_SIZE=2g \
    MAX_DIRECT_MEMORY_SIZE=2g

# Foreground, so the JVM is pid 1's child and SIGTERM reaches it: the supervisor owns the only
# shutdown hook and needs the signal to drain Cassandra and close the OpenSearch node in order.
ENTRYPOINT ["/opt/cassandra-opensearch/bin/cassandra-opensearch"]
CMD ["start"]

# Both services have to answer, not just the process: `status` exits 0 only once the supervisor
# MBean is registered, which is the last step of the startup sequence.
HEALTHCHECK --interval=30s --timeout=10s --start-period=180s --retries=3 \
    CMD ["/opt/cassandra-opensearch/bin/cassandra-opensearch", "status", "--quiet"]
