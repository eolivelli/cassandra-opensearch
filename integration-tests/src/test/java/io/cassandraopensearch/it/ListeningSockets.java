/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Which TCP ports a given process is listening on, asked of the operating system.
 *
 * <p>This exists to prove the product's central claim from the outside. That Cassandra and
 * OpenSearch are in one JVM is asserted everywhere inside the process and nowhere outside it, and
 * an assertion made from inside cannot distinguish "one JVM" from "two JVMs that agree". Matching
 * both servers' listening sockets to a single pid can.
 *
 * <p>Implemented against {@code /proc} rather than by shelling out to {@code ss} or {@code lsof}:
 * those are not installed everywhere, their output formats differ between versions, and the
 * information needed — socket inode to port, then file descriptor to socket inode — is two flat
 * files away.
 */
final class ListeningSockets {

    /** {@code TCP_LISTEN} in {@code include/net/tcp_states.h}, as {@code /proc/net/tcp} prints it. */
    private static final String LISTEN = "0A";

    private ListeningSockets() {
    }

    static boolean available() {
        return Files.isReadable(Path.of("/proc/net/tcp"));
    }

    /** @return every port the process is listening on, IPv4 and IPv6 */
    static Set<Integer> of(long pid) {
        Map<Long, Integer> portsByInode = new HashMap<>();
        portsByInode.putAll(listeningPortsByInode(Path.of("/proc/net/tcp")));
        portsByInode.putAll(listeningPortsByInode(Path.of("/proc/net/tcp6")));

        Set<Integer> ports = new HashSet<>();
        Path descriptors = Path.of("/proc", String.valueOf(pid), "fd");
        if (!Files.isDirectory(descriptors)) {
            return ports;
        }
        try (Stream<Path> entries = Files.list(descriptors)) {
            entries.forEach(entry -> inodeOf(entry).map(portsByInode::get).ifPresent(ports::add));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot inspect " + descriptors, e);
        }
        return ports;
    }

    private static Map<Long, Integer> listeningPortsByInode(Path procFile) {
        Map<Long, Integer> ports = new HashMap<>();
        if (!Files.isReadable(procFile)) {
            return ports;
        }
        try (Stream<String> lines = Files.lines(procFile)) {
            lines.skip(1).forEach(line -> {
                String[] fields = line.trim().split("\\s+");
                // sl, local_address, rem_address, st, tx:rx, tr:when, retrnsmt, uid, timeout, inode
                if (fields.length > 9 && LISTEN.equals(fields[3])) {
                    String local = fields[1];
                    ports.put(Long.parseLong(fields[9]),
                            Integer.parseInt(local.substring(local.indexOf(':') + 1), 16));
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + procFile, e);
        }
        return ports;
    }

    private static Optional<Long> inodeOf(Path descriptor) {
        try {
            String target = Files.readSymbolicLink(descriptor).toString();
            if (!target.startsWith("socket:[")) {
                return Optional.empty();
            }
            return Optional.of(
                    Long.parseLong(target.substring("socket:[".length(), target.length() - 1)));
        } catch (IOException | RuntimeException e) {
            // File descriptors come and go while the directory is being listed; one that has
            // vanished is not an error.
            return Optional.empty();
        }
    }
}
