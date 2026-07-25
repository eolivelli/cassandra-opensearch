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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Line edits to a shipped configuration file, in place.
 *
 * <p>Text rather than a YAML round trip on purpose. Re-emitting {@code conf/cassandra.yaml}
 * through a parser would drop every comment and re-order every key, and the file the node then
 * read would no longer be the file the distribution ships — which is the one thing these tests
 * exist to exercise. The keys touched here are all plain scalars at a known indentation.
 *
 * <p>Every edit asserts that it changed something. A rewrite that silently matched nothing is the
 * worst outcome available: the node would come up on the stock ports, collide with whatever the
 * host already has there, and fail somewhere far away from the cause.
 */
final class ConfigFile {

    private final Path path;
    private String text;

    ConfigFile(Path path) {
        this.path = path;
        try {
            this.text = Files.readString(path);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + path, e);
        }
    }

    /**
     * Replaces the value of a scalar key wherever it appears, keeping its indentation.
     *
     * <p>Anchoring on the start of the line is what keeps {@code transport_port} from also
     * matching {@code native_transport_port}, and what leaves commented-out sample keys alone.
     */
    ConfigFile set(String key, Object value) {
        return replace(Pattern.compile("(?m)^([ \t]*)" + Pattern.quote(key) + ":[^\n]*$"),
                "$1" + key + ": " + value, key);
    }

    /** The one line inside {@code seed_provider}'s nested parameter list. */
    ConfigFile setSeeds(String seeds) {
        return replace(Pattern.compile("(?m)^([ \t]*)- seeds:[^\n]*$"),
                "$1- seeds: \"" + seeds + '"', "- seeds");
    }

    /** Replaces a whole line, matched by its start, with any number of lines. */
    ConfigFile replaceLineStartingWith(String prefix, String replacement) {
        return replace(Pattern.compile("(?m)^" + Pattern.quote(prefix) + "[^\n]*$"),
                Matcher.quoteReplacement(replacement), prefix);
    }

    /** Adds a line immediately below an existing key, at the same indentation. */
    ConfigFile addAfter(String key, String addedKey, Object value) {
        return replace(Pattern.compile("(?m)^([ \t]*)" + Pattern.quote(key) + ":([^\n]*)$"),
                "$1" + key + ":$2\n$1" + addedKey + ": " + value, key);
    }

    private ConfigFile replace(Pattern pattern, String replacement, String what) {
        Matcher matcher = pattern.matcher(text);
        // The match, not the resulting text, is what is checked: a key whose shipped value
        // already happens to equal the new one would otherwise look like a failed edit.
        if (!matcher.find()) {
            throw new IllegalStateException(
                    "nothing to replace for '" + what + "' in " + path
                            + "; the shipped file has changed shape and this node would have"
                            + " started on the wrong address or port");
        }
        text = matcher.reset().replaceAll(replacement);
        return this;
    }

    void save() {
        try {
            Files.writeString(path, text);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write " + path, e);
        }
    }
}
