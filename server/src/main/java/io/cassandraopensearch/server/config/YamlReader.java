/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.server.config;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Typed, path-aware access to a parsed YAML mapping.
 *
 * <p>Every error names the file and the full dotted key, because a configuration error message
 * that says only {@code "expected a number"} forces the operator to go hunting. Unknown keys are
 * rejected via {@link #rejectUnknownKeys}: a typo in an ops config file should stop the process,
 * not silently leave a limit unset.
 */
final class YamlReader {

    private final Path source;
    private final String path;
    private final Map<String, Object> values;

    YamlReader(Path source, Map<String, Object> values) {
        this(source, "", values);
    }

    private YamlReader(Path source, String path, Map<String, Object> values) {
        this.source = source;
        this.path = path;
        this.values = values;
    }

    /**
     * @param optional when true, a missing section yields an empty reader and defaults apply;
     *                 when false, a missing section is an error
     */
    @SuppressWarnings("unchecked")
    YamlReader section(String key, boolean optional) {
        Object value = values.get(key);
        if (value == null) {
            if (optional) {
                return new YamlReader(source, qualify(key), Map.of());
            }
            throw new ConfigurationException(
                    "Missing required section '" + qualify(key) + "' in " + source + ".");
        }
        if (!(value instanceof Map)) {
            throw new ConfigurationException(
                    "'" + qualify(key) + "' in " + source + " must be a mapping, found "
                            + describe(value) + ".");
        }
        return new YamlReader(source, qualify(key), (Map<String, Object>) value);
    }

    String string(String key, String defaultValue) {
        Object value = values.get(key);
        return value == null ? defaultValue : String.valueOf(value);
    }

    boolean bool(String key, boolean defaultValue) {
        Object value = values.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean b) {
            return b;
        }
        throw new ConfigurationException(
                "'" + qualify(key) + "' in " + source + " must be true or false, found "
                        + describe(value) + ".");
    }

    int port(String key, int defaultValue) {
        int value = integer(key, defaultValue);
        if (value < 1 || value > 65535) {
            throw new ConfigurationException(
                    "'" + qualify(key) + "' in " + source + " must be a port between 1 and 65535,"
                            + " found " + value + ".");
        }
        return value;
    }

    /**
     * Reads a whole number.
     *
     * <p>Narrowing is refused rather than performed. {@code Number.intValue()} would turn {@code
     * 9200.7} into {@code 9200} and {@code 4294967296} into {@code 0} — the second of which is
     * then reported by {@link #port} as "found 0", naming a value the operator never wrote and
     * sending them looking for a bug that is not there. Both are typos, and both stop the process.
     */
    int integer(String key, int defaultValue) {
        Object value = values.get(key);
        if (value == null) {
            return defaultValue;
        }
        BigInteger whole = wholeNumber(value);
        if (whole == null) {
            throw new ConfigurationException(
                    "'" + qualify(key) + "' in " + source + " must be a whole number, found "
                            + describe(value) + " '" + value + "'.");
        }
        if (whole.bitLength() >= Integer.SIZE) {
            throw new ConfigurationException(
                    "'" + qualify(key) + "' in " + source + " must be between " + Integer.MIN_VALUE
                            + " and " + Integer.MAX_VALUE + ", found " + whole + ".");
        }
        return whole.intValue();
    }

    /** The value as an exact integer, or null when it is not an integral type at all. */
    private static BigInteger wholeNumber(Object value) {
        if (value instanceof BigInteger big) {
            return big;
        }
        // snakeyaml's SafeConstructor yields Integer, Long or BigInteger for an integer literal
        // and Double for anything with a decimal point or an exponent.
        if (value instanceof Integer || value instanceof Long
                || value instanceof Short || value instanceof Byte) {
            return BigInteger.valueOf(((Number) value).longValue());
        }
        return null;
    }

    /**
     * Reads a duration written the way the rest of the Cassandra and OpenSearch ecosystem writes
     * them: {@code 30s}, {@code 5m}, {@code 2h}, {@code 500ms}. A bare number is rejected rather
     * than assumed to be seconds, since guessing the unit is how timeouts end up 1000x wrong.
     *
     * <p>Zero and negative are rejected too. Every duration in this file is a limit on how long
     * something may take or how often it repeats, and none of them has a sensible meaning at zero
     * — {@code health_check_interval: 0s} used to reach {@code scheduleWithFixedDelay} and fail
     * there with a bare {@code IllegalArgumentException: null}, hundreds of lines away from the
     * key that caused it.
     */
    Duration duration(String key, Duration defaultValue) {
        Object value = values.get(key);
        if (value == null) {
            return defaultValue;
        }
        String text = String.valueOf(value).trim();
        Duration parsed = parseDuration(key, text);
        if (parsed.isZero() || parsed.isNegative()) {
            throw new ConfigurationException(
                    "'" + qualify(key) + "' in " + source + " must be a positive duration, found '"
                            + text + "'.");
        }
        return parsed;
    }

    private Duration parseDuration(String key, String text) {
        try {
            if (text.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(text.substring(0, text.length() - 2).trim()));
            }
            if (text.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(text.substring(0, text.length() - 1).trim()));
            }
            if (text.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(text.substring(0, text.length() - 1).trim()));
            }
            if (text.endsWith("h")) {
                return Duration.ofHours(Long.parseLong(text.substring(0, text.length() - 1).trim()));
            }
        } catch (NumberFormatException e) {
            throw new ConfigurationException(
                    "'" + qualify(key) + "' in " + source + " is not a valid duration: '" + text + "'.", e);
        }
        throw new ConfigurationException(
                "'" + qualify(key) + "' in " + source + " must carry a unit — one of ms, s, m, h"
                        + " (for example '30s' or '5m') — found '" + text + "'.");
    }

    /**
     * Fails if the mapping holds any key outside {@code known}.
     *
     * @throws ConfigurationException listing the unknown keys and what was expected
     */
    void rejectUnknownKeys(Collection<String> known) {
        Set<String> unknown = new LinkedHashSet<>(values.keySet());
        unknown.removeAll(known);
        if (!unknown.isEmpty()) {
            throw new ConfigurationException(
                    "Unknown setting" + (unknown.size() == 1 ? " " : "s ") + unknown
                            + (path.isEmpty() ? "" : " in section '" + path + "'")
                            + " of " + source + ". Known settings here: " + known + ".");
        }
    }

    private String qualify(String key) {
        return path.isEmpty() ? key : path + "." + key;
    }

    private static String describe(Object value) {
        return value.getClass().getSimpleName().toLowerCase();
    }
}
