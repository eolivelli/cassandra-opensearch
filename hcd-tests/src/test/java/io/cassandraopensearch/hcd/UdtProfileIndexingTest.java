/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.cassandraopensearch.hcd;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end proof that Cassandra UDTs (User-Defined Types) are replicated to OpenSearch
 * by the HCD OpenSearch interceptor and are searchable via the OpenSearch REST API.
 *
 * <p>The scenario models a Hogwarts student registry: a user-profile table for Harry Potter
 * characters with three multi-value UDT columns:
 * <ul>
 *   <li>{@code addresses} — {@code list<frozen<address>>} — home address + school address</li>
 *   <li>{@code emails}    — {@code list<frozen<email>>}   — personal and school owl-mail</li>
 *   <li>{@code phones}    — {@code list<frozen<phone>>}   — home and mobile numbers</li>
 * </ul>
 *
 * <p>The test proves the full data path in order:
 * <ol>
 *   <li>Schema — UDTs and table are created in Cassandra</li>
 *   <li>Index  — {@code CREATE CUSTOM INDEX ... USING 'OpenSearchIndex'} wires replication
 *                and auto-creates the OpenSearch index with Cassandra-derived mappings</li>
 *   <li>Write  — CQL INSERTs for Harry, Hermione, and Draco reach Cassandra</li>
 *   <li>Replicated — documents appear in OpenSearch without any direct OS write</li>
 *   <li>Search — characters are findable by house, city, email, and phone number</li>
 * </ol>
 *
 * <p><strong>Test ordering is mandatory.</strong> Tests 2–9 depend on the schema, index, and
 * data created by tests 1–2. Do not move, skip, or remove any {@code @Order} annotation.
 * {@code @BeforeAll}/{@code @AfterAll} execute in their normal lifecycle positions regardless
 * of {@code @Order} — that annotation governs {@code @Test} methods only.
 *
 * <p>Requires the HCD + OpenSearch stack from {@code docker-compose-full.yaml} to be running.
 * Run with:
 * <pre>{@code
 *   mvn test -pl hcd-tests -Phcd-tests --no-transfer-progress
 * }</pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class UdtProfileIndexingTest {

    private static final String CQL_HOST   = System.getProperty("hcd.host",       "localhost");
    private static final int    CQL_PORT   = Integer.parseInt(System.getProperty("hcd.cql.port",  "9042"));
    private static final String DATACENTER = System.getProperty("hcd.datacenter", "datacenter1");

    // Unique suffix keeps this test's keyspace/index isolated from any other run.
    private static final String RUN_ID     = String.valueOf(Instant.now().toEpochMilli());
    private static final String KEYSPACE   = "hogwarts_" + RUN_ID;
    private static final String TABLE      = "student_profile";
    private static final String INDEX_NAME = "hogwarts-students-" + RUN_ID;
    private static final String CQL_INDEX  = "student_profile_os_idx";

    // ── Harry Potter — Gryffindor ─────────────────────────────────────────────
    private static final String HARRY_ID   = "550e8400-e29b-41d4-a716-446655440001";
    private static final String HARRY_NAME = "Harry Potter";

    // ── Hermione Granger — Gryffindor ─────────────────────────────────────────
    private static final String HERMIONE_ID   = "550e8400-e29b-41d4-a716-446655440002";
    private static final String HERMIONE_NAME = "Hermione Granger";

    // ── Draco Malfoy — Slytherin ──────────────────────────────────────────────
    private static final String DRACO_ID   = "550e8400-e29b-41d4-a716-446655440003";
    private static final String DRACO_NAME = "Draco Malfoy";

    // Matches the integer value of hits.total.value in an OpenSearch _search response.
    // Uses a word-boundary-equivalent pattern (\D) to avoid "value":1 matching "value":10.
    private static final Pattern DOC_COUNT = Pattern.compile("\"value\":(\\d+)");

    private CqlSession session;
    private final OpenSearchClient os = new OpenSearchClient();

    // ── lifecycle ────────────────────────────────────────────────────────────

    @BeforeAll
    void connect() {
        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(CQL_HOST, CQL_PORT))
                .withLocalDatacenter(DATACENTER)
                // Fail fast when the stack is not running; the default driver timeout is 5 s
                // per contact point with no diagnostic message about which host was tried.
                .withTimeout(Duration.ofSeconds(5))
                .build();
    }

    @AfterAll
    void cleanup() {
        // Best-effort teardown: drop the keyspace (cascades to table + CQL index)
        // and the OpenSearch index so repeated test runs start clean.
        try {
            if (session != null && !session.isClosed()) {
                session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            }
        } finally {
            // A 404 is expected if the test failed before the index was created.
            // Any other non-200 status is unexpected and logged as a warning.
            OpenSearchClient.Response del = os.delete("/" + INDEX_NAME);
            if (del.status() != 200 && del.status() != 404) {
                System.err.println("WARN: cleanup DELETE /" + INDEX_NAME
                        + " returned " + del.status() + ": " + del.body());
            }
            if (session != null) {
                session.close();
            }
        }
    }

    // ── tests ────────────────────────────────────────────────────────────────

    /**
     * Creates the keyspace, UDT types, and table.
     *
     * <p>UDTs are defined before the table that references them. {@code columnsToIndex}
     * is used instead of the parenthesised column list because Cassandra's secondary-index
     * validator rejects non-frozen UDT columns via the parenthesis path (README §2).
     * A {@code house} column is included so searches can filter by Gryffindor / Slytherin.
     */
    @Test
    @Order(1)
    void createSchemaAndOpenSearchIndex() {
        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
                + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");

        session.execute(
                "CREATE TYPE IF NOT EXISTS " + KEYSPACE + ".address ("
                + "  street text, city text, state text, zip text)");
        session.execute(
                "CREATE TYPE IF NOT EXISTS " + KEYSPACE + ".email ("
                + "  label text, address text)");
        session.execute(
                "CREATE TYPE IF NOT EXISTS " + KEYSPACE + ".phone ("
                + "  label text, number text)");

        session.execute(
                "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + " ("
                + "  user_id   uuid PRIMARY KEY,"
                + "  full_name text,"
                + "  house     text,"
                + "  addresses list<frozen<address>>,"
                + "  emails    list<frozen<email>>,"
                + "  phones    list<frozen<phone>>)");

        // CREATE CUSTOM INDEX wires the HCD OpenSearch interceptor to this table and
        // auto-creates the OpenSearch index with Cassandra-derived field mappings.
        // columnsToIndex is used to explicitly name the UDT list columns because
        // Cassandra's index validator may reject frozen-collection columns passed
        // via the parenthesis syntax.
        session.execute(
                "CREATE CUSTOM INDEX IF NOT EXISTS " + CQL_INDEX
                + " ON " + KEYSPACE + "." + TABLE + " ()"
                + " USING 'OpenSearchIndex'"
                + " WITH OPTIONS = {"
                + "   'indexName': '" + INDEX_NAME + "',"
                + "   'createIndexIfNotExists': 'true',"
                + "   'numShards': '1',"
                + "   'numReplicas': '0',"
                + "   'columnsToIndex': 'full_name,house,addresses,emails,phones'"
                + " }");

        // Confirm the OpenSearch index was created by the interceptor.
        OpenSearchClient.Response stats = os.get("/" + INDEX_NAME + "/_stats");
        assertThat(stats.status())
                .as("OpenSearch index '%s' should exist after CREATE CUSTOM INDEX", INDEX_NAME)
                .isEqualTo(200);
    }

    /**
     * Inserts three Hogwarts students — Harry Potter, Hermione Granger, and Draco Malfoy —
     * each with a home address, a school address at Hogwarts, two email addresses, and two
     * phone numbers. Asserts each row is readable back over CQL immediately after insert.
     */
    @Test
    @Order(2)
    void insertStudentProfilesViaCql() {
        // Harry Potter — 4 Privet Drive / Hogwarts, Gryffindor
        // NOTE: Unquoted UDT field names in inline literals (street, city, …) are accepted by
        // HCD 2.0.8-SNAPSHOT's CQL parser. If this INSERT fails with a parse error on a
        // different HCD version, switch to SimpleStatement with named bindings or a prepared
        // statement, which are immune to parser grammar differences.
        session.execute(
                "INSERT INTO " + KEYSPACE + "." + TABLE
                + " (user_id, full_name, house, addresses, emails, phones) VALUES ("
                + "  " + HARRY_ID + ","
                + "  'Harry Potter',"
                + "  'Gryffindor',"
                + "  [{street:'4 Privet Drive',          city:'Little Whinging', state:'Surrey',    zip:'GU25 1AA'},"
                + "   {street:'Gryffindor Tower',         city:'Hogwarts',        state:'Scotland',  zip:'FK17 8LR'}],"
                + "  [{label:'owl',    address:'hpotter@hogwarts.ac.uk'},"
                + "   {label:'muggle', address:'harry.potter@gmail.com'}],"
                + "  [{label:'home',   number:'+441483550101'},"
                + "   {label:'mobile', number:'+447700900001'}]"
                + ")");

        // Hermione Granger — Hampstead / Hogwarts, Gryffindor
        session.execute(
                "INSERT INTO " + KEYSPACE + "." + TABLE
                + " (user_id, full_name, house, addresses, emails, phones) VALUES ("
                + "  " + HERMIONE_ID + ","
                + "  'Hermione Granger',"
                + "  'Gryffindor',"
                + "  [{street:'12 Hampstead Lane',        city:'London',          state:'England',   zip:'N6 4RS'},"
                + "   {street:'Gryffindor Tower',         city:'Hogwarts',        state:'Scotland',  zip:'FK17 8LR'}],"
                + "  [{label:'owl',    address:'hgranger@hogwarts.ac.uk'},"
                + "   {label:'muggle', address:'hermione.granger@gmail.com'}],"
                + "  [{label:'home',   number:'+442074460202'},"
                + "   {label:'mobile', number:'+447700900002'}]"
                + ")");

        // Draco Malfoy — Malfoy Manor / Hogwarts, Slytherin
        session.execute(
                "INSERT INTO " + KEYSPACE + "." + TABLE
                + " (user_id, full_name, house, addresses, emails, phones) VALUES ("
                + "  " + DRACO_ID + ","
                + "  'Draco Malfoy',"
                + "  'Slytherin',"
                + "  [{street:'Malfoy Manor, Wiltshire',  city:'Wiltshire',       state:'England',   zip:'SN8 1AA'},"
                + "   {street:'Slytherin Dungeon',         city:'Hogwarts',        state:'Scotland',  zip:'FK17 8LR'}],"
                + "  [{label:'owl',    address:'dmalfoy@hogwarts.ac.uk'},"
                + "   {label:'muggle', address:'draco.malfoy@gmail.com'}],"
                + "  [{label:'home',   number:'+441672520303'},"
                + "   {label:'mobile', number:'+447700900003'}]"
                + ")");

        // Verify all three rows are immediately readable over CQL.
        for (String[] pair : new String[][] {
                {HARRY_ID,    HARRY_NAME},
                {HERMIONE_ID, HERMIONE_NAME},
                {DRACO_ID,    DRACO_NAME}}) {
            Row row = session.execute(
                    "SELECT full_name FROM " + KEYSPACE + "." + TABLE
                    + " WHERE user_id = " + pair[0]).one();
            assertThat(row).as("row for %s must exist in Cassandra", pair[1]).isNotNull();
            assertThat(row.getString("full_name")).isEqualTo(pair[1]);
        }
    }

    /**
     * Polls OpenSearch until all three student documents appear, proving the HCD interceptor
     * replicated the CQL writes without any direct OpenSearch write from this test.
     *
     * <p>Replication is asynchronous — the interceptor batches and sends mutations in the
     * background. A 30-second deadline is generous for a local stack; typical latency is
     * under one second.
     */
    @Test
    @Order(3)
    void allThreeStudentsAreReplicatedToOpenSearch() {
        awaitDocumentCount(3, "all three student profiles must be replicated within 30 s");

        // Spot-check: each character's name appears in the index.
        OpenSearchClient.Response all = os.post("/" + INDEX_NAME + "/_search",
                "{\"query\":{\"match_all\":{}},\"size\":10}");
        assertThat(all.body())
                .contains(HARRY_NAME)
                .contains(HERMIONE_NAME)
                .contains(DRACO_NAME);
    }

    /**
     * Searches by Hogwarts house — both Gryffindors must be returned, Draco must not.
     */
    @Test
    @Order(4)
    void searchByHouse() {
        OpenSearchClient.Response response = os.post("/" + INDEX_NAME + "/_search",
                "{\"query\":{\"match\":{\"house\":\"Gryffindor\"}}}");

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body())
                .as("Gryffindor search must return Harry and Hermione")
                .contains(HARRY_NAME)
                .contains(HERMIONE_NAME)
                .doesNotContain(DRACO_NAME);
    }

    /**
     * Searches by {@code addresses.city} UDT field — "Little Whinging" is Harry's home city.
     *
     * <p>HCD maps {@code list<frozen<address>>} to an object array in OpenSearch.
     * The field path uses dot notation: {@code addresses.city}.
     *
     * <p>Uses a plain {@code match} query rather than {@code match_phrase} because "Little
     * Whinging" is a two-token value and neither token appears in any other student's city
     * in this data set ("London", "Wiltshire", "Hogwarts"). If new students were added with
     * a city containing "Little" or "Whinging", this assertion would need to become
     * {@code match_phrase}.
     */
    @Test
    @Order(5)
    void searchByAddressCity() {
        OpenSearchClient.Response response = os.post("/" + INDEX_NAME + "/_search",
                "{\"query\":{\"match\":{\"addresses.city\":\"Little Whinging\"}}}");

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body())
                .as("search by addresses.city='Little Whinging' must return only Harry")
                .contains(HARRY_NAME)
                .doesNotContain(HERMIONE_NAME)
                .doesNotContain(DRACO_NAME);
    }

    /**
     * Searches by {@code addresses.city} for "Hogwarts" — all three students share
     * their school address, so all three must be returned.
     */
    @Test
    @Order(6)
    void searchBySharedSchoolCity() {
        OpenSearchClient.Response response = os.post("/" + INDEX_NAME + "/_search",
                "{\"query\":{\"match\":{\"addresses.city\":\"Hogwarts\"}}}");

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body())
                .as("all students have a Hogwarts address")
                .contains(HARRY_NAME)
                .contains(HERMIONE_NAME)
                .contains(DRACO_NAME);
    }

    /**
     * Searches by {@code emails.address} UDT field — Hermione's school owl-mail address.
     *
     * <p>Uses {@code match_phrase} so all tokens must appear in order. A plain {@code match}
     * would tokenise the address and match "hogwarts" across all three students. HCD's
     * dynamic template maps UDT string sub-fields to {@code text} without a {@code .keyword}
     * sub-field, so {@code term} cannot be used here.
     */
    @Test
    @Order(7)
    void searchByEmailAddress() {
        OpenSearchClient.Response response = os.post("/" + INDEX_NAME + "/_search",
                "{\"query\":{\"match_phrase\":{\"emails.address\":\"hgranger@hogwarts.ac.uk\"}}}");

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body())
                .as("phrase email match must return only Hermione")
                .contains(HERMIONE_NAME)
                .doesNotContain(HARRY_NAME)
                .doesNotContain(DRACO_NAME);
    }

    /**
     * Searches by {@code phones.number} UDT field — Draco's mobile number.
     *
     * <p>Uses {@code match_phrase} because HCD's dynamic template maps UDT string
     * sub-fields to {@code text} (no {@code .keyword} sub-field). The full phone number
     * is unique per character so a phrase match is exact in practice.
     */
    @Test
    @Order(8)
    void searchByPhoneNumber() {
        OpenSearchClient.Response response = os.post("/" + INDEX_NAME + "/_search",
                "{\"query\":{\"match_phrase\":{\"phones.number\":\"+447700900003\"}}}");

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body())
                .as("phrase phone number match must return only Draco")
                .contains(DRACO_NAME)
                .doesNotContain(HARRY_NAME)
                .doesNotContain(HERMIONE_NAME);
    }

    /**
     * Full-text search on {@code full_name} — "Potter" must find Harry only.
     */
    @Test
    @Order(9)
    void searchByFullName() {
        OpenSearchClient.Response response = os.post("/" + INDEX_NAME + "/_search",
                "{\"query\":{\"match\":{\"full_name\":\"Potter\"}}}");

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.body())
                .as("full-text search for 'Potter' must return Harry")
                .contains(HARRY_NAME)
                .doesNotContain(HERMIONE_NAME)
                .doesNotContain(DRACO_NAME);
    }

    /**
     * Fetches Harry Potter's record from both Cassandra (CQL) and OpenSearch (REST) and
     * asserts the field values agree — proving the HCD interceptor replicates faithfully
     * without mutating the data.
     *
     * <p>Three fields are compared across both protocols:
     * <ul>
     *   <li>{@code full_name} / {@code full_name} — top-level text, has a {@code .keyword} sub-field</li>
     *   <li>{@code house}     / {@code house}      — top-level text</li>
     *   <li>first element of {@code addresses.city} — UDT list sub-field, plain {@code text}</li>
     * </ul>
     *
     * <p>The OpenSearch document {@code _id} is the Cassandra {@code user_id} UUID (set by the HCD
     * interceptor at replication time), so {@code GET /<index>/_doc/<uuid>} is the direct read.
     */
    @Test
    @Order(10)
    void dualReadComparesOneStudentAcrossBothProtocols() {
        // ── Read from Cassandra over CQL ──────────────────────────────────────
        Row cqlRow = session.execute(
                "SELECT full_name, house, addresses FROM " + KEYSPACE + "." + TABLE
                + " WHERE user_id = " + HARRY_ID).one();

        assertThat(cqlRow).as("Harry's CQL row must exist").isNotNull();

        String cqlName  = cqlRow.getString("full_name");
        String cqlHouse = cqlRow.getString("house");
        // addresses is list<frozen<address>>; get the first element's city field.
        UdtValue firstAddress = cqlRow.getList("addresses", UdtValue.class).get(0);
        String cqlCity = firstAddress.getString("city");

        // ── Read from OpenSearch by document id (= Cassandra user_id) ─────────
        // The HCD interceptor sets the document _id to the row's partition key UUID.
        OpenSearchClient.Response osGet = os.get("/" + INDEX_NAME + "/_doc/" + HARRY_ID);
        assertThat(osGet.status())
                .as("GET by user_id '%s' must return 200 — document must exist in OpenSearch", HARRY_ID)
                .isEqualTo(200);
        assertThat(osGet.body())
                .as("OpenSearch document must be found")
                .contains("\"found\":true");

        // ── Cross-protocol field comparison ───────────────────────────────────
        // Assert that the three CQL values appear verbatim in the OpenSearch _source.
        // Uses simple substring matching to avoid a JSON parser dependency in this module.
        assertThat(osGet.body())
                .as("OpenSearch full_name must match CQL full_name = \"%s\"", cqlName)
                .contains("\"full_name\":\"" + cqlName + "\"");
        assertThat(osGet.body())
                .as("OpenSearch house must match CQL house = \"%s\"", cqlHouse)
                .contains("\"house\":\"" + cqlHouse + "\"");
        assertThat(osGet.body())
                .as("OpenSearch addresses[0].city must match CQL addresses[0].city = \"%s\"", cqlCity)
                .contains("\"city\":\"" + cqlCity + "\"");
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /**
     * Polls {@code /_search?size=0} until {@code hits.total.value} reaches {@code expected},
     * refreshing the index on each attempt. Fails with the last observed count at the deadline.
     */
    private void awaitDocumentCount(int expected, String description) {
        Duration timeout  = Duration.ofSeconds(30);
        Duration interval = Duration.ofMillis(500);
        Instant  deadline = Instant.now().plus(timeout);
        String   lastBody = "";

        while (Instant.now().isBefore(deadline)) {
            os.post("/" + INDEX_NAME + "/_refresh", null);
            OpenSearchClient.Response r = os.post("/" + INDEX_NAME + "/_search",
                    "{\"query\":{\"match_all\":{}},\"size\":0}");
            lastBody = r.body();
            if (r.status() == 200) {
                Matcher m = DOC_COUNT.matcher(lastBody);
                if (m.find() && Integer.parseInt(m.group(1)) == expected) {
                    return;
                }
            }
            sleep(interval);
        }
        throw new AssertionError(description + "\n  last response: " + lastBody);
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for replication", e);
        }
    }
}
