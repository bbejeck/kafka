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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.FaultRule;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
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
import java.util.List;
import java.util.Properties;
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

        streams = new KafkaStreams(builder.build(), props);
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
