/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.RocksDBStoreCorruptionUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the KIP-1035 on-disk store open/closed invariant and the recovery path when a store is
 * left marked {@code OPEN} on disk (the fingerprint of an unclean shutdown / kill -9).
 *
 * <p>The store keeps its lifecycle state in the RocksDB {@code offsets} column family under a
 * {@code status} key ({@code 1L} == open, {@code 0L} == closed). Two properties are exercised:
 * <ol>
 *   <li><b>Invariant:</b> a clean {@link KafkaStreams#close()} must always leave every persistent
 *       store marked {@code CLOSED} on disk.</li>
 *   <li><b>Recovery:</b> if a store is nonetheless reopened under EOS with a stale {@code OPEN}
 *       marker, Streams must treat the task as corrupted, wipe + restore from the changelog, and
 *       return to {@code RUNNING} with the correct materialized counts -- i.e. no crash loop.</li>
 * </ol>
 *
 * @see RocksDBStoreCorruptionUtils
 */
@Tag("integration")
@Timeout(600)
public class RocksDBOpenMarkerRecoveryIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBOpenMarkerRecoveryIntegrationTest.class);

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private static final String STORE_NAME = "count-store";
    private static final int NUM_PARTITIONS = 2;
    private static final int NUM_KEYS = 3;
    private static final int RECORDS_PER_KEY = 200;
    private static final int TOTAL_RECORDS = NUM_KEYS * RECORDS_PER_KEY;
    // <stateDir>/<appId>/<taskId>/rocksdb/<storeName>, taskId looks like "0_0".
    private static final Pattern TASK_DIR_PATTERN = Pattern.compile("\\d+_\\d+");

    private String appId;
    private String inputTopic;
    private String outputTopic;
    private File stateDir;

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Test
    public void shouldMarkStoreClosedOnCleanCloseAndRecoverFromStaleOpenMarker() throws Exception {
        // Phase 1: run cleanly, then assert the CLOSED-on-clean-close invariant.
        try (final KafkaStreams streams = new KafkaStreams(buildTopology().build(), streamsConfig())) {
            streams.cleanUp();
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(
                java.util.Collections.singletonList(streams), Duration.ofSeconds(60));

            produceRecords();
            TestUtils.waitForCondition(
                () -> storeCountSum(streams) == TOTAL_RECORDS,
                60_000L,
                () -> "Store did not reach " + TOTAL_RECORDS + " counts before clean close");

            streams.close(Duration.ofSeconds(30));
        }

        final List<File> storeDirs = storeDirs();
        assertFalse(storeDirs.isEmpty(), "Expected at least one persistent store directory");
        for (final File storeDir : storeDirs) {
            assertEquals(0L, RocksDBStoreCorruptionUtils.readStatus(storeDir),
                "INVARIANT VIOLATED: clean close must leave " + storeDir + " marked CLOSED (0L)");
        }

        // Phase 2: doctor each store back to OPEN on disk (kill -9 fingerprint) and reopen.
        for (final File storeDir : storeDirs) {
            RocksDBStoreCorruptionUtils.setStoreStatusToOpen(storeDir);
            assertEquals(1L, RocksDBStoreCorruptionUtils.readStatus(storeDir),
                "Setup did not take: " + storeDir + " should be OPEN (1L) before restart");
        }

        final List<Throwable> uncaught = new CopyOnWriteArrayList<>();
        try (final KafkaStreams streams = new KafkaStreams(buildTopology().build(), streamsConfig())) {
            // Do NOT cleanUp() -- we must reopen the doctored on-disk state.
            streams.setUncaughtExceptionHandler(throwable -> {
                uncaught.add(throwable);
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            });
            streams.start();

            // The stale OPEN marker under EOS should trigger TaskCorrupted -> wipe -> restore from
            // changelog, and the app should return to RUNNING with the correct counts: no crash loop.
            try {
                TestUtils.waitForCondition(
                    () -> streams.state() == KafkaStreams.State.RUNNING && storeCountSum(streams) == TOTAL_RECORDS,
                    120_000L,
                    () -> "Did not recover to RUNNING with " + TOTAL_RECORDS + " counts; state="
                        + streams.state() + ", uncaught=" + uncaught);
            } catch (final Throwable t) {
                LOG.error("Recovery failed. Final state={}, uncaught throwables={}", streams.state(), uncaught, t);
                throw t;
            }

            // After recovery the stores must once again be openable and, on the next clean close,
            // marked CLOSED -- confirming the reopen wrote a fresh OPEN then CLOSED (no lingering stale marker).
            streams.close(Duration.ofSeconds(30));
        }
        for (final File storeDir : storeDirs()) {
            assertEquals(0L, RocksDBStoreCorruptionUtils.readStatus(storeDir),
                "Post-recovery clean close must leave " + storeDir + " marked CLOSED (0L)");
        }
    }

    @Test
    public void shouldRecoverWhenCommittedOffsetIsAheadOfChangelog() throws Exception {
        final long bump = 10_000L;

        // Phase 1: build the store cleanly so it persists real changelog offsets.
        try (final KafkaStreams streams = new KafkaStreams(buildTopology().build(), streamsConfig())) {
            streams.cleanUp();
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(
                java.util.Collections.singletonList(streams), Duration.ofSeconds(60));

            produceRecords();
            TestUtils.waitForCondition(
                () -> storeCountSum(streams) == TOTAL_RECORDS,
                60_000L,
                () -> "Store did not reach " + TOTAL_RECORDS + " counts before clean close");

            streams.close(Duration.ofSeconds(30));
        }

        // Push each persisted changelog offset well past the changelog log-end (partial-commit fingerprint).
        final List<File> storeDirs = storeDirs();
        assertFalse(storeDirs.isEmpty(), "Expected at least one persistent store directory");
        final Map<File, Map<String, Long>> injectedOffsets = new HashMap<>();
        for (final File storeDir : storeDirs) {
            final Map<String, Long> committed = RocksDBStoreCorruptionUtils.readCommittedOffsets(storeDir);
            assertFalse(committed.isEmpty(),
                "Clean close should persist changelog offsets for " + storeDir);
            final Map<String, Long> ahead = new HashMap<>();
            for (final Map.Entry<String, Long> entry : committed.entrySet()) {
                final long aheadOffset = entry.getValue() + bump;
                RocksDBStoreCorruptionUtils.writeCommittedOffset(storeDir, entry.getKey(), aheadOffset);
                ahead.put(entry.getKey(), aheadOffset);
            }
            assertEquals(ahead, RocksDBStoreCorruptionUtils.readCommittedOffsets(storeDir),
                "Ahead-offset setup did not take for " + storeDir);
            injectedOffsets.put(storeDir, ahead);
        }

        // Phase 2: reopen with the ahead offset and keep processing.
        final List<Throwable> uncaught = new CopyOnWriteArrayList<>();
        try (final KafkaStreams streams = new KafkaStreams(buildTopology().build(), streamsConfig())) {
            // Do NOT cleanUp() -- reopen the doctored on-disk state.
            streams.setUncaughtExceptionHandler(throwable -> {
                uncaught.add(throwable);
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            });
            streams.start();

            // The store is CLOSED and its offset is >= the changelog log-end, so it is treated as
            // caught-up: no restore, no data loss, no crash loop.
            try {
                TestUtils.waitForCondition(
                    () -> streams.state() == KafkaStreams.State.RUNNING && storeCountSum(streams) == TOTAL_RECORDS,
                    120_000L,
                    () -> "Did not reopen to RUNNING with " + TOTAL_RECORDS + " counts; state="
                        + streams.state() + ", uncaught=" + uncaught);

                // Process another full batch; counts must advance, proving the store keeps working and
                // real commits overwrite the bogus ahead offset.
                produceRecords();
                TestUtils.waitForCondition(
                    () -> storeCountSum(streams) == 2 * TOTAL_RECORDS,
                    120_000L,
                    () -> "Store did not advance to " + (2 * TOTAL_RECORDS) + " counts; state="
                        + streams.state() + ", uncaught=" + uncaught);
            } catch (final Throwable t) {
                LOG.error("Ahead-offset recovery failed. Final state={}, uncaught={}", streams.state(), uncaught, t);
                throw t;
            }

            streams.close(Duration.ofSeconds(30));
        }

        // Clean close leaves CLOSED, and the injected ahead offset must have been corrected downward
        // by the real commits during phase 2 (no lingering bogus offset on disk).
        for (final Map.Entry<File, Map<String, Long>> injected : injectedOffsets.entrySet()) {
            final File storeDir = injected.getKey();
            assertEquals(0L, RocksDBStoreCorruptionUtils.readStatus(storeDir),
                "Post-recovery clean close must leave " + storeDir + " marked CLOSED (0L)");
            final Map<String, Long> current = RocksDBStoreCorruptionUtils.readCommittedOffsets(storeDir);
            for (final Map.Entry<String, Long> aheadEntry : injected.getValue().entrySet()) {
                final Long currentOffset = current.get(aheadEntry.getKey());
                assertNotNull(currentOffset,
                    "Changelog offset missing after recovery for " + aheadEntry.getKey() + " in " + storeDir);
                assertTrue(currentOffset < aheadEntry.getValue(),
                    "Injected ahead offset was not corrected for " + aheadEntry.getKey() + " in " + storeDir
                        + ": on-disk=" + currentOffset + ", injected=" + aheadEntry.getValue());
            }
        }
    }

    @Test
    public void shouldRecoverWhenCommittedOffsetsAreDeleted() throws Exception {
        buildInitialStoreAndCloseCleanly();

        // Drop the persisted changelog offsets (keeps the CLOSED status marker). On reopen the store has
        // no committed offset, so it must restore from the start of the changelog.
        final List<File> storeDirs = storeDirs();
        assertFalse(storeDirs.isEmpty(), "Expected at least one persistent store directory");
        for (final File storeDir : storeDirs) {
            assertFalse(RocksDBStoreCorruptionUtils.readCommittedOffsets(storeDir).isEmpty(),
                "Clean close should persist changelog offsets for " + storeDir);
            RocksDBStoreCorruptionUtils.deleteOffsets(storeDir);
            assertTrue(RocksDBStoreCorruptionUtils.readCommittedOffsets(storeDir).isEmpty(),
                "Offset deletion did not take for " + storeDir);
        }

        final List<Throwable> uncaught = new CopyOnWriteArrayList<>();
        try (final KafkaStreams streams = new KafkaStreams(buildTopology().build(), streamsConfig())) {
            // Do NOT cleanUp() -- reopen the doctored on-disk state.
            streams.setUncaughtExceptionHandler(throwable -> {
                uncaught.add(throwable);
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            });
            streams.start();

            // Missing offset -> restore from changelog start rebuilds the same counts (changelog is a
            // compacted key->count log, so replay is idempotent): no data loss, no crash loop.
            try {
                TestUtils.waitForCondition(
                    () -> streams.state() == KafkaStreams.State.RUNNING && storeCountSum(streams) == TOTAL_RECORDS,
                    120_000L,
                    () -> "Did not recover to RUNNING with " + TOTAL_RECORDS + " counts; state="
                        + streams.state() + ", uncaught=" + uncaught);

                produceRecords();
                TestUtils.waitForCondition(
                    () -> storeCountSum(streams) == 2 * TOTAL_RECORDS,
                    120_000L,
                    () -> "Store did not advance to " + (2 * TOTAL_RECORDS) + " counts; state="
                        + streams.state() + ", uncaught=" + uncaught);
            } catch (final Throwable t) {
                LOG.error("Deleted-offset recovery failed. Final state={}, uncaught={}", streams.state(), uncaught, t);
                throw t;
            }

            streams.close(Duration.ofSeconds(30));
        }

        for (final File storeDir : storeDirs()) {
            assertEquals(0L, RocksDBStoreCorruptionUtils.readStatus(storeDir),
                "Post-recovery clean close must leave " + storeDir + " marked CLOSED (0L)");
            assertFalse(RocksDBStoreCorruptionUtils.readCommittedOffsets(storeDir).isEmpty(),
                "Recovery should re-persist changelog offsets for " + storeDir);
        }
    }

    @Test
    public void shouldRecoverWhenCommittedOffsetValueIsCorrupt() throws Exception {
        buildInitialStoreAndCloseCleanly();

        // Corrupt each persisted changelog offset value to a wrong-length blob (keeps CLOSED status and
        // the position). On reopen the store reads this committed offset and the Long deserializer fails.
        final List<File> storeDirs = storeDirs();
        assertFalse(storeDirs.isEmpty(), "Expected at least one persistent store directory");
        for (final File storeDir : storeDirs) {
            final Map<String, Long> committed = RocksDBStoreCorruptionUtils.readCommittedOffsets(storeDir);
            assertFalse(committed.isEmpty(),
                "Clean close should persist changelog offsets for " + storeDir);
            for (final String topicPartition : committed.keySet()) {
                RocksDBStoreCorruptionUtils.corruptOffset(storeDir, topicPartition);
            }
        }

        final List<Throwable> uncaught = new CopyOnWriteArrayList<>();
        try (final KafkaStreams streams = new KafkaStreams(buildTopology().build(), streamsConfig())) {
            // Do NOT cleanUp() -- reopen the doctored on-disk state.
            streams.setUncaughtExceptionHandler(throwable -> {
                uncaught.add(throwable);
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            });
            streams.start();

            // A corrupt committed-offset value must be treated as corruption (wipe + restore), not
            // escape as a raw deserialization error: the store should recover to RUNNING with the
            // correct counts.
            try {
                TestUtils.waitForCondition(
                    () -> streams.state() == KafkaStreams.State.RUNNING && storeCountSum(streams) == TOTAL_RECORDS,
                    120_000L,
                    () -> "Did not recover to RUNNING with " + TOTAL_RECORDS + " counts; state="
                        + streams.state() + ", uncaught=" + uncaught);
            } catch (final Throwable t) {
                LOG.error("Corrupt-offset recovery failed. Final state={}, uncaught={}", streams.state(), uncaught, t);
                throw t;
            }

            streams.close(Duration.ofSeconds(30));
        }

        for (final File storeDir : storeDirs()) {
            assertEquals(0L, RocksDBStoreCorruptionUtils.readStatus(storeDir),
                "Post-recovery clean close must leave " + storeDir + " marked CLOSED (0L)");
        }
    }

    @Test
    public void shouldRecoverWhenPositionIsCorrupt() throws Exception {
        buildInitialStoreAndCloseCleanly();

        // Corrupt the on-disk IQ position vector (keeps the CLOSED status marker and the offsets).
        final List<File> storeDirs = storeDirs();
        assertFalse(storeDirs.isEmpty(), "Expected at least one persistent store directory");
        for (final File storeDir : storeDirs) {
            RocksDBStoreCorruptionUtils.corruptPosition(storeDir);
        }

        final List<Throwable> uncaught = new CopyOnWriteArrayList<>();
        try (final KafkaStreams streams = new KafkaStreams(buildTopology().build(), streamsConfig())) {
            // Do NOT cleanUp() -- reopen the doctored on-disk state.
            streams.setUncaughtExceptionHandler(throwable -> {
                uncaught.add(throwable);
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            });
            streams.start();

            // A corrupt position must not silently serve a wrong IQ bound and must not crash-loop:
            // the store should recover to RUNNING with the correct counts.
            try {
                TestUtils.waitForCondition(
                    () -> streams.state() == KafkaStreams.State.RUNNING && storeCountSum(streams) == TOTAL_RECORDS,
                    120_000L,
                    () -> "Did not recover to RUNNING with " + TOTAL_RECORDS + " counts; state="
                        + streams.state() + ", uncaught=" + uncaught);
            } catch (final Throwable t) {
                LOG.error("Corrupt-position recovery failed. Final state={}, uncaught={}", streams.state(), uncaught, t);
                throw t;
            }

            streams.close(Duration.ofSeconds(30));
        }

        for (final File storeDir : storeDirs()) {
            assertEquals(0L, RocksDBStoreCorruptionUtils.readStatus(storeDir),
                "Post-recovery clean close must leave " + storeDir + " marked CLOSED (0L)");
        }
    }

    @BeforeEach
    public void setUp() throws InterruptedException {
        appId = "rocksdb-open-marker-recovery-" + TestUtils.randomString(6);
        // Unique topics per test so a new-appId (earliest) reader never picks up another test's records.
        inputTopic = appId + "-input";
        outputTopic = appId + "-output";
        // Reused across both app instances so the second run sees the first run's on-disk store.
        stateDir = TestUtils.tempDirectory();
        CLUSTER.createTopic(inputTopic, NUM_PARTITIONS, 1);
        CLUSTER.createTopic(outputTopic, NUM_PARTITIONS, 1);
    }

    private Properties streamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private StreamsBuilder buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        // Input is already keyed, so count() needs no repartition -> a single sub-topology,
        // one persistent RocksDB store per partition backed by the count-store changelog.
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count(Materialized.<String, Long>as(Stores.persistentKeyValueStore(STORE_NAME))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()))
            .toStream()
            .to(outputTopic);
        return builder;
    }

    // Build the store to TOTAL_RECORDS counts and shut down cleanly (leaving CLOSED, consistent offsets).
    private void buildInitialStoreAndCloseCleanly() throws Exception {
        try (final KafkaStreams streams = new KafkaStreams(buildTopology().build(), streamsConfig())) {
            streams.cleanUp();
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(
                java.util.Collections.singletonList(streams), Duration.ofSeconds(60));
            produceRecords();
            TestUtils.waitForCondition(
                () -> storeCountSum(streams) == TOTAL_RECORDS,
                60_000L,
                () -> "Store did not reach " + TOTAL_RECORDS + " counts before clean close");
            streams.close(Duration.ofSeconds(30));
        }
    }

    private void produceRecords() {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final List<KeyValue<String, String>> records = new ArrayList<>(TOTAL_RECORDS);
        for (int k = 0; k < NUM_KEYS; k++) {
            final String key = "key-" + k;
            final int key0 = k;
            IntStream.range(0, RECORDS_PER_KEY)
                .forEach(i -> records.add(KeyValue.pair(key, "v-" + key0 + "-" + i)));
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopic, records, producerConfig, CLUSTER.time);
    }

    // Sum of all per-key counts in the materialized store; equals the number of input records once
    // everything has been processed (count() emits one running count per key).
    private long storeCountSum(final KafkaStreams streams) throws Exception {
        final ReadOnlyKeyValueStore<String, Long> store = IntegrationTestUtils.getStore(
            STORE_NAME, streams, QueryableStoreTypes.keyValueStore());
        long sum = 0L;
        try (final KeyValueIterator<String, Long> all = store.all()) {
            while (all.hasNext()) {
                sum += all.next().value;
            }
        }
        return sum;
    }

    // Resolve every active-task RocksDB store dir: <stateDir>/<appId>/<taskId>/rocksdb/<storeName>.
    private List<File> storeDirs() {
        final File appDir = new File(stateDir, appId);
        final File[] taskDirs = appDir.listFiles(
            f -> f.isDirectory() && TASK_DIR_PATTERN.matcher(f.getName()).matches());
        assertTrue(taskDirs != null && taskDirs.length > 0,
            "No task directories found under " + appDir);
        return java.util.Arrays.stream(taskDirs)
            .map(taskDir -> new File(new File(taskDir, "rocksdb"), STORE_NAME))
            .filter(File::isDirectory)
            .collect(Collectors.toList());
    }
}