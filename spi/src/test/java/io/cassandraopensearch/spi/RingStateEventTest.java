/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.spi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The grammar of {@code ring.state.changed}. It is a contract between two modules that share no
 * code, written by the Cassandra runtime and read by the supervisor, and the supervisor's whole
 * reaction to a node leaving its ring hangs off one field of it.
 */
class RingStateEventTest {

    @Test
    void roundTripsThroughItsOwnMessageFormat() {
        RingStateEvent event = new RingStateEvent("LEAVING", true, "operation mode moved NORMAL -> LEAVING");

        assertThat(RingStateEvent.parse(event.toMessage())).isEqualTo(event);
    }

    @Test
    void roundTripsWhenTheNodeIsNotLeaving() {
        RingStateEvent event = new RingStateEvent("MOVING", false, "moved NORMAL -> MOVING");

        assertThat(RingStateEvent.parse(event.toMessage())).isEqualTo(event);
    }

    @Test
    void readsTheFieldsOutOfAMessage() {
        RingStateEvent event = RingStateEvent.parse("state=DECOMMISSIONED leaving=true; moved LEAVING -> DECOMMISSIONED");

        assertThat(event).isNotNull();
        assertThat(event.state()).isEqualTo("DECOMMISSIONED");
        assertThat(event.leaving()).isTrue();
        assertThat(event.detail()).isEqualTo("moved LEAVING -> DECOMMISSIONED");
    }

    @Test
    void aDetailMayContainTheSeparator() {
        RingStateEvent event = RingStateEvent.parse("state=LEAVING leaving=true; drained; streaming");

        assertThat(event).isNotNull();
        assertThat(event.detail()).isEqualTo("drained; streaming");
    }

    @Test
    void aMissingDetailIsEmptyRatherThanNull() {
        RingStateEvent event = RingStateEvent.parse("state=LEAVING leaving=true");

        assertThat(event).isNotNull();
        assertThat(event.detail()).isEmpty();
    }

    /**
     * The defect. {@code leaving} was read with {@link Boolean#parseBoolean}, which maps every
     * value it does not recognise to {@code false} — and {@code false} is the verdict "this node
     * is not on its way out, do nothing". A message that says {@code leaving=yes} therefore did
     * not fail: it silently switched off the watchdog that excludes the node from OpenSearch
     * shard allocation, which is the one thing standing between an unsupervised {@code nodetool
     * decommission} and shards that never relocate.
     */
    @ParameterizedTest
    @ValueSource(strings = {"yes", "no", "1", "0", "TRUE", "True", "FALSE", "y", "t", "", "maybe"})
    void aLeavingFlagThatIsNotExactlyTrueOrFalseRejectsTheMessage(String leaving) {
        assertThat(RingStateEvent.parse("state=LEAVING leaving=" + leaving + "; whatever"))
                .as("guessing at '%s' would act on the wrong thing; refusing to parse does not", leaving)
                .isNull();
    }

    @Test
    void aMessageWithNoLeavingFlagIsRejected() {
        assertThat(RingStateEvent.parse("state=LEAVING; no flag here")).isNull();
    }

    @Test
    void aMessageWithNoStateIsRejected() {
        assertThat(RingStateEvent.parse("leaving=true; no state here")).isNull();
        assertThat(RingStateEvent.parse("state= leaving=true; empty state")).isNull();
    }

    @Test
    void aMessageOutsideTheGrammarIsRejectedRatherThanGuessedAt() {
        assertThat(RingStateEvent.parse(null)).isNull();
        assertThat(RingStateEvent.parse("")).isNull();
        assertThat(RingStateEvent.parse("the node is leaving the ring")).isNull();
    }

    @Test
    void extraWhitespaceAndFieldOrderDoNotMatter() {
        RingStateEvent event = RingStateEvent.parse("  leaving=false   state=NORMAL  ; settled");

        assertThat(event).isNotNull();
        assertThat(event.state()).isEqualTo("NORMAL");
        assertThat(event.leaving()).isFalse();
        assertThat(event.detail()).isEqualTo("settled");
    }

    @Test
    void theTypeIsTheOneTheSupervisorFiltersOn() {
        assertThat(RingStateEvent.TYPE).isEqualTo("ring.state.changed");
    }
}
