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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.ClientFault;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.FaultInjectingClientSupplier;
import org.apache.kafka.streams.integration.utils.FaultInjectingClientSupplier.ProducerCall;
import org.apache.kafka.streams.integration.utils.FaultRule;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.TaskManager;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies KIP-892 transactional state stores survive broker-side errors injected mid-commit, using the
 * {@link KafkaProtocolFaultProxy}. Two shapes, both under {@code exactly_once_v2 + enable.transactional.statestores}:
 *
 * <ol>
 *   <li><b>Deterministic one-shot</b> — inject a single retriable {@code EndTxn} error; assert the app never
 *       goes {@code ERROR}, the fault actually fired, and the count store still converges to the correct
 *       totals (KIP-892's commit/retry path works as advertised).</li>
 *   <li><b>Continuous chaos window</b> — arm a probabilistic {@code EndTxn} fault while streaming, then
 *       disarm and assert eventual convergence (the store must not corrupt or lose data under repeated
 *       transient commit failures).</li>
 *   <li><b>Commit-boundary hammering</b> — fault the commit ({@code EndTxn}) heavily across many keys with a
 *       deterministic burst of retriable errors, then assert exact per-key counts and an exactly-once total
 *       (no lost or double-counted records) — commit-boundary <em>retries</em> don't corrupt the store.</li>
 *   <li><b>Fatal fault → TaskCorrupted → rollback + restore</b> — inject a fatal {@code commitTransaction}
 *       exception (via the {@link FaultInjectingClientSupplier}) so Streams marks the task corrupted, aborts
 *       the in-flight transaction, discards the uncommitted state-store writes, and restores the store from
 *       the changelog. Assert the app recovers to {@code RUNNING}, the corruption/restore path was actually
 *       taken (log signature), and the final counts are exactly-once (reprocessing after rollback neither
 *       loses nor double-counts).</li>
 *   <li><b>Producer fenced → TaskMigrated → rejoin</b> — a broker-returned {@code PRODUCER_FENCED} at the
 *       commit boundary (injected on the wire) must be handled as a task migration: the thread closes its
 *       tasks and rejoins the group, recovering to {@code RUNNING} with exactly-once totals. Exercises the
 *       general EOS fencing path, distinct from the corruption path above.</li>
 * </ol>
 */
@Timeout(300)
@Tag("integration")
public class TransactionalStoreFaultInjectionIntegrationTest {

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    private String inputTopic;
    private String outputTopic;
    private String appId;
    private KafkaProtocolFaultProxy proxy;
    private KafkaStreams streams;
    private final AtomicReference<Throwable> uncaught = new AtomicReference<>();

    @BeforeAll
    public static void startCluster() throws Exception {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        final String base = safeUniqueTestName(info);
        appId = "txn-fault-" + base;
        inputTopic = appId + "-in";
        outputTopic = appId + "-out";
        CLUSTER.createTopic(inputTopic, 2, 1);
        CLUSTER.createTopic(outputTopic, 2, 1);
        proxy = KafkaProtocolFaultProxy.inFrontOf(CLUSTER.bootstrapServers());
    }

    @AfterEach
    public void tearDown() {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
            streams.cleanUp();
        }
        if (proxy != null) {
            proxy.close();
        }
    }

    private void startCountApp() throws Exception {
        startCountApp(null);
    }

    /**
     * Start the count app. When {@code supplier} is non-null the app is built with it (used to inject
     * client-side producer faults); otherwise the default supplier is used. Either way the app is routed
     * through the proxy, so wire-level faults can be armed independently.
     */
    private void startCountApp(final KafkaClientSupplier supplier) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .groupByKey(Grouped.with(Serdes.Integer(), Serdes.Integer()))
            .count(Materialized.as("counts"))     // persistent RocksDB, transactional under EOS
            .toStream()
            .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.Long()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        // Route the whole app through the proxy.
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, true);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streams = supplier == null
            ? new KafkaStreams(builder.build(), props)
            : new KafkaStreams(builder.build(), props, supplier);
        streams.setUncaughtExceptionHandler(t -> {
            uncaught.compareAndSet(null, t);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        startApplicationAndWaitUntilRunning(streams);
    }

    /** A single transient EndTxn error must be transparently retried; totals stay correct, app stays up. */
    @ParameterizedTest
    @ValueSource(strings = {"CONCURRENT_TRANSACTIONS", "COORDINATOR_LOAD_IN_PROGRESS"})
    public void shouldSurviveSingleTransientCommitError(final String errorName) throws Exception {
        startCountApp();
        final FaultRule rule = proxy.injectError(ApiKeys.END_TXN, Errors.valueOf(errorName)).once();

        // Two records for key=1 -> committed count must reach 2 despite one failed-then-retried commit.
        produce(1, 2);

        final List<KeyValue<Integer, Long>> out = waitUntilMinKeyValueRecordsReceived(
            consumerConfig(), outputTopic, 1, 60_000);
        final long finalCount = out.stream().filter(kv -> kv.key == 1).mapToLong(kv -> kv.value).max().orElse(-1);

        assertEquals(2L, finalCount, "count store must converge to 2 after the retried commit");
        assertTrue(rule.timesTriggered() >= 1, "the injected EndTxn error should have fired at least once");
        assertNull(uncaught.get(), uncaught.get() == null ? "" : "app crashed:\n" + stack(uncaught.get()));
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "app should still be RUNNING");
    }

    /** Under a continuous probabilistic EndTxn fault window, the store must not corrupt or lose data. */
    @Test
    public void shouldConvergeUnderContinuousCommitFaultWindow() throws Exception {
        startCountApp();

        // Open a chaos window: ~30% of EndTxns fail transiently, continuously, while we stream.
        final FaultRule chaos = proxy.injectError(ApiKeys.END_TXN, Errors.COORDINATOR_LOAD_IN_PROGRESS)
            .withProbability(0.3);
        produce(7, 25);
        // Give the app time to churn through retries under the fault window.
        Thread.sleep(3_000);
        chaos.remove(); // close the window; subsequent commits are clean

        // After the window closes the count for key=7 must converge to exactly 25 (no loss, no double-count).
        final List<KeyValue<Integer, Long>> out = waitUntilMinKeyValueRecordsReceived(
            consumerConfig(), outputTopic, 1, 120_000);
        final long finalCount = out.stream().filter(kv -> kv.key == 7).mapToLong(kv -> kv.value).max().orElse(-1);

        assertEquals(25L, finalCount, "count must converge to 25 after the chaos window");
        assertNull(uncaught.get(), uncaught.get() == null ? "" : "app crashed:\n" + stack(uncaught.get()));
    }

    /**
     * Hammer the EOS commit boundary ({@code EndTxn}) with retriable errors while streaming many keys. A
     * retriable error at the commit boundary is retried inside the producer's {@code commitTransaction()} (it
     * delays, rather than aborts, the transaction), so this proves that heavy commit-boundary retrying does not
     * corrupt the transactional store: every key still ends at its exact count and the grand total is
     * exactly-once (no lost or double-counted records).
     *
     * <p>The fault is a <em>deterministic bounded burst</em> ({@code times(n)}) rather than probabilistic: this
     * guarantees the faults actually fire (no flaky "maybe zero fired") while staying bounded so the producer's
     * retries eventually succeed and the app makes progress. (The transaction-<em>abort</em>/buffer-rollback
     * path is a fatal fault, exercised separately.)
     *
     * <p>Note: {@code EndTxn} is the commit-boundary request that Streams actually issues under transaction
     * protocol v2 — {@code AddOffsetsToTxn} is no longer sent as a separate request, so it is not a useful
     * injection point here.
     */
    @Test
    public void shouldMaintainExactlyOnceUnderCommitBoundaryHammering() throws Exception {
        startCountApp();
        final int numKeys = 10;
        final int perKey = 20; // 200 records total, spread across keys

        // Arm before producing so the very first commit is hit. times(n) fires on the first n EndTxn requests,
        // then lets retries through — deterministic yet bounded.
        final FaultRule endTxn = proxy.injectError(ApiKeys.END_TXN, Errors.COORDINATOR_LOAD_IN_PROGRESS).times(5);

        produceAcrossKeys(numKeys, perKey);

        final Map<Integer, Long> finalCounts = readFinalCounts(numKeys, perKey, 150_000);
        for (int k = 0; k < numKeys; k++) {
            assertEquals((long) perKey, finalCounts.get(k),
                "key " + k + " must count exactly " + perKey + " (got " + finalCounts + ")");
        }
        assertEquals((long) numKeys * perKey,
            finalCounts.values().stream().mapToLong(Long::longValue).sum(),
            "total across all keys must equal the exactly-once input count");
        // The app must commit at least once to produce output, so the armed burst is guaranteed to fire.
        assertTrue(endTxn.timesTriggered() >= 1, "the EndTxn fault should have fired");
        assertNull(uncaught.get(), uncaught.get() == null ? "" : "app crashed:\n" + stack(uncaught.get()));
        assertEquals(KafkaStreams.State.RUNNING, streams.state(), "app should still be RUNNING");
    }

    /**
     * A fatal {@code commitTransaction} failure must corrupt the task, roll back the uncommitted transactional
     * state-store writes, restore the store from the changelog, and reprocess — landing on exactly-once totals.
     *
     * <p>This exercises the KIP-892 recovery path that a retriable-error test can't: a {@code TimeoutException}
     * from {@code commitTransaction()} under EOSv2 is mapped by Streams to a {@code TaskCorruptedException}
     * ({@code TaskExecutor.commitOffsetsOrTransaction}), which aborts the in-flight txn and revives the task
     * from its changelog. We inject that exception with the {@link FaultInjectingClientSupplier} (a client-side
     * fault the wire proxy can't express), then prove: (a) the corruption/restore path was actually taken
     * (TaskManager log), (b) the app returns to {@code RUNNING}, and (c) counts are exactly-once — the second
     * batch, whose commit was corrupted and rolled back, is reprocessed to the correct total, never doubled.
     */
    @Test
    public void shouldRollBackAndRestoreExactlyOnceWhenCommitCorruptsTask() throws Exception {
        final FaultInjectingClientSupplier supplier =
            FaultInjectingClientSupplier.wrapping(new DefaultKafkaClientSupplier());
        startCountApp(supplier);

        final int numKeys = 5;

        // Batch 1: converge to 10 so a real commit persists data to the store AND the changelog.
        produceAcrossKeys(numKeys, 10);
        readFinalCounts(numKeys, 10, 60_000);

        try (LogCaptureAppender logs = LogCaptureAppender.createAndRegister(TaskManager.class)) {
            // Force the NEXT commit to fail fatally: TimeoutException -> TaskCorrupted -> abort + restore.
            final ClientFault fault = supplier.failOn(ProducerCall.COMMIT_TRANSACTION,
                () -> new TimeoutException("injected fatal commit failure")).once();

            // Batch 2: +10 per key. The commit after processing these corrupts the task; after rollback the
            // uncommitted increments are discarded, the store is restored from the changelog, and batch 2 is
            // reprocessed from the last committed offset -> exactly-once final count of 20 (never 30).
            produceAcrossKeys(numKeys, 10);
            final Map<Integer, Long> finalCounts = readFinalCounts(numKeys, 20, 120_000);

            for (int k = 0; k < numKeys; k++) {
                assertEquals(20L, finalCounts.get(k),
                    "key " + k + " must be exactly-once 20 after corruption+restore (got " + finalCounts + ")");
            }
            assertEquals(1, fault.timesTriggered(), "the fatal commit fault should have fired exactly once");
            assertTrue(
                logs.getMessages().stream().anyMatch(m -> m.toLowerCase(Locale.ROOT).contains("corrupt")),
                "TaskManager should have logged handling a corrupted task (restore path actually taken)");
        }
        // Reviving a corrupted task transiently drives the app through REBALANCING; wait for it to settle.
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.RUNNING,
            60_000L,
            () -> "app should have recovered to RUNNING; state=" + streams.state()
                + (uncaught.get() == null ? "" : ", crashed:\n" + stack(uncaught.get())));
        assertNull(uncaught.get(), uncaught.get() == null ? "" : "app crashed:\n" + stack(uncaught.get()));
    }

    /**
     * A broker-returned {@code PRODUCER_FENCED} at the commit boundary must be handled as a
     * <em>TaskMigrated</em> (not a corruption or a crash): Streams closes its tasks, rejoins the consumer
     * group, and reprocesses — landing on exactly-once totals. This exercises the general EOS fencing path
     * (distinct from the {@code TimeoutException}→TaskCorrupted path), driven through the wire proxy since
     * {@code PRODUCER_FENCED} is a genuine broker error code.
     *
     * <p>{@code StreamsProducer.commitTransaction} maps {@code ProducerFencedException} from {@code EndTxn} to
     * {@code TaskMigratedException}, which {@code StreamThread.handleTaskMigrated} resolves by
     * {@code handleLostAll()} + re-subscribe (rejoin). We assert the app recovers to {@code RUNNING}, the
     * fence/rejoin path was actually taken (log signature), and the counts are exactly-once.
     */
    @Test
    public void shouldRecoverViaRebalanceWhenProducerFencedAtCommit() throws Exception {
        startCountApp(); // default supplier; PRODUCER_FENCED is injected on the wire by the proxy
        final int numKeys = 5;

        // Batch 1: converge to 10 so a real commit persists data + changelog before we fence.
        produceAcrossKeys(numKeys, 10);
        readFinalCounts(numKeys, 10, 60_000);

        try (LogCaptureAppender logs = LogCaptureAppender.createAndRegister(StreamThread.class)) {
            // The broker really commits, but the proxy rewrites the next EndTxn response to PRODUCER_FENCED,
            // which the client surfaces as a fence -> Streams treats the task as migrated and rejoins.
            final FaultRule fenced = proxy.injectError(ApiKeys.END_TXN, Errors.PRODUCER_FENCED).once();

            // Batch 2: +10 per key. The commit after processing them is fenced; the thread rejoins the group
            // and reprocesses from the last committed offset -> exactly-once final count of 20 (never 30).
            produceAcrossKeys(numKeys, 10);
            final Map<Integer, Long> finalCounts = readFinalCounts(numKeys, 20, 120_000);

            for (int k = 0; k < numKeys; k++) {
                assertEquals(20L, finalCounts.get(k),
                    "key " + k + " must be exactly-once 20 after fence+rejoin (got " + finalCounts + ")");
            }
            assertEquals(1, fenced.timesTriggered(), "the PRODUCER_FENCED fault should have fired exactly once");
            assertTrue(
                logs.getMessages().stream().anyMatch(m -> m.toLowerCase(Locale.ROOT).contains("being fenced")),
                "StreamThread should have handled a TaskMigrated (fence) and rejoined the group");
        }
        // The fence-driven rejoin transiently drives the app through REBALANCING; wait for it to settle.
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.RUNNING,
            60_000L,
            () -> "app should have recovered to RUNNING; state=" + streams.state()
                + (uncaught.get() == null ? "" : ", crashed:\n" + stack(uncaught.get())));
        assertNull(uncaught.get(), uncaught.get() == null ? "" : "app crashed:\n" + stack(uncaught.get()));
    }

    private void produceAcrossKeys(final int numKeys, final int perKey) {
        final Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        final List<KeyValue<Integer, Integer>> records = new ArrayList<>();
        // Interleave keys so commits span multiple keys' updates (round-robin).
        for (int i = 0; i < perKey; i++) {
            for (int k = 0; k < numKeys; k++) {
                records.add(new KeyValue<>(k, k));
            }
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopic, records, p, org.apache.kafka.common.utils.Time.SYSTEM);
    }

    /** Poll the (read_committed) output until every key has reached {@code perKey}; return the max per key. */
    private Map<Integer, Long> readFinalCounts(final int numKeys, final int perKey, final long timeoutMs) throws Exception {
        final Map<Integer, Long> maxPerKey = new ConcurrentHashMap<>();
        final Properties cfg = consumerConfig();
        // Fresh group each call so repeated reads start from the beginning and stay independent.
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + appId + "-" + java.util.UUID.randomUUID());
        try (Consumer<Integer, Long> consumer = new KafkaConsumer<>(cfg)) {
            consumer.subscribe(List.of(outputTopic));
            TestUtils.waitForCondition(() -> {
                final ConsumerRecords<Integer, Long> records = consumer.poll(Duration.ofMillis(200));
                for (final ConsumerRecord<Integer, Long> r : records) {
                    maxPerKey.merge(r.key(), r.value(), Math::max);
                }
                return maxPerKey.size() == numKeys
                    && maxPerKey.values().stream().allMatch(v -> v >= perKey);
            }, timeoutMs, () -> "did not observe final counts; so far: " + maxPerKey);
        }
        return maxPerKey;
    }

    private void produce(final int key, final int count) {
        final Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()); // producer talks to broker directly
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        final List<KeyValue<Integer, Integer>> records = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new KeyValue<>(key, key));
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, records, p, org.apache.kafka.common.utils.Time.SYSTEM);
    }

    private Properties consumerConfig() {
        final Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + appId);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return c;
    }

    private static String stack(final Throwable t) {
        final StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
