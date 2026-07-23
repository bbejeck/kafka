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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.FaultRule;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * RUNTIME chaos harness — an extended-duration, two-instance sibling of
 * {@link RuntimeChaosCombinedSoakIntegrationTest}. Differences from the base lever, kept as small as possible:
 *
 * <ol>
 *     <li>TWO EOS-v2 instances with {@code num.standby.replicas=1} (not one) -- with only one instance, the
 *     task movement below has no standby to hand a task off to, so most of the interesting KIP-1035
 *     task-handoff paths would never fire. Every fault still comes from the wire proxy -- nothing is
 *     self-inflicted by the topology.</li>
 *     <li>A dedicated thread-shed cycle ({@code runThreadShedCycles}) forces real rebalances and task movement
 *     on top of the broker-level wire faults below: it alternately calls
 *     {@link KafkaStreams#removeStreamThread()} then {@link KafkaStreams#addStreamThread()} on each instance
 *     (as {@link RuntimeChaosCleanerRaceIntegrationTest} does), so the shed thread's tasks must be picked up
 *     elsewhere and later get reassigned back. Graceful and in-group -- unlike a network partition, it never
 *     evicts a member from the consumer group by session timeout, avoiding the "this member's local topic
 *     metadata is stale" churn a full blackhole/rejoin can trigger. This is what actually exercises the
 *     KIP-1035 task-handoff paths; the wire faults alone mostly just retry without ever moving a task between
 *     instances.</li>
 *     <li>The windowed store's {@link TimeWindows} carries a real GRACE_MS grace period (instead of
 *     {@code ofSizeWithNoGrace}), so a fault-induced delay doesn't get silently dropped as "late" data.</li>
 *     <li>The chaos duration is read from a system property ({@link #DURATION_PROPERTY}); pass a larger value
 *     (or {@code <= 0}) for a long-running manual soak, e.g. {@code -Pchaos.indefiniteSoak.duration.ms=<millis>}
 *     (the gradle {@code integrationTest} task forwards this one project property to the forked test JVM; a
 *     plain {@code -D} on the gradle command line does not reach that JVM).</li>
 *     <li>Window-store retention ({@code RETENTION_MS}) must exceed the actual wall-clock length of a run
 *     (chaos duration + drain + final IQ wait), or the oldest windows age out of the store before the final
 *     reconciliation and the sum falsely looks short. Currently sized for roughly an hour-scale run; raise it
 *     (and {@code @Timeout} below to match) for a longer manual soak.</li>
 * </ol>
 *
 * <p>Everything else -- the exactly-once oracle -- is unchanged from the base lever; the fault probabilities
 * below are raised relative to it. See its javadoc for the full rationale.
 */
@Tag("integration")
@Timeout(7_200) // 2h hang-guard, matched to the current 1h-scale RETENTION_MS with headroom
public class RuntimeChaosIndefiniteSoakIntegrationTest {

    private static final String STORE_NAME = "chaos-windowed-counts";
    private static final String[] KEYS = {"a", "b", "c"};
    private static final long WINDOW_MS = 200L;
    private static final long GRACE_MS = 20_000L; // tolerate chaos-induced delay before a record is "late"
    // 1h -- must still exceed actual run length (chaos duration + drain + IQ wait); raise for a longer soak
    private static final long RETENTION_MS = 3_600_000L;
    private static final String DURATION_PROPERTY = "chaos.indefiniteSoak.duration.ms";
    private static final long DEFAULT_CHAOS_DURATION_MS = 90_000L; // override via -P...; <=0 = run until interrupted
    private static final String CLIENT_A = "instA";
    private static final String CLIENT_B = "instB";
    // How long to leave a stream thread shed before adding it back: long enough for the rebalance + task
    // reassignment it triggers to actually complete.
    private static final long MOVEMENT_WINDOW_MS = 15_000L;
    private static final long MOVEMENT_SETTLE_MS = 5_000L; // dwell after adding the thread back before the next cycle

    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private String inputTopic;
    private String outputTopic;
    private String appId;
    private KafkaStreams streamsA;
    private KafkaStreams streamsB;
    private KafkaProducer<String, String> producer;

    private final AtomicReference<Throwable> uncaughtA = new AtomicReference<>();
    private final AtomicReference<Throwable> uncaughtB = new AtomicReference<>();
    private final AtomicLong movementCycles = new AtomicLong(0);
    private final AtomicLong produced = new AtomicLong(0);
    private final AtomicBoolean producing = new AtomicBoolean(false);
    private Thread producerThread;

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        final String base = safeUniqueTestName(info);
        appId = "chaos-indefinite-soak-" + base;
        inputTopic = appId + "-in";
        outputTopic = appId + "-out";
        cluster.createTopic(inputTopic, 6, 1);
        cluster.createTopic(outputTopic, 6, 1);
        // 6 partitions split across 2 instances -> both active and standby tasks exist, more churn surface
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());

        final Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(p);
    }

    @AfterEach
    public void tearDown() throws Exception {
        producing.set(false);
        if (producerThread != null) {
            producerThread.join(Duration.ofSeconds(10).toMillis());
        }
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        if (streamsA != null) {
            streamsA.close(Duration.ofSeconds(30));
        }
        if (streamsB != null) {
            streamsB.close(Duration.ofSeconds(30));
        }
        if (proxy != null) {
            proxy.close();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    public void shouldStayExactlyOnceUnderCombinedRuntimeChaos() throws Exception {
        startApp();
        startProducer();

        // Combined runtime chaos: several faults armed at once, all withProbability, so rare interleavings can
        // occur (a corruption landing mid-migration, a commit gap during a revive, a produce retry across a
        // rebalance). Probabilities raised well above the base combined-soak lever's to push much more chaos
        // through per unit time; tuned so the app still makes progress and can drain when cleared. NO restart.
        final FaultRule restoreOor = proxy.injectError(ApiKeys.FETCH, Errors.OFFSET_OUT_OF_RANGE)
            .forClient("restore").withProbability(0.8);
        final FaultRule fence = proxy.injectError(ApiKeys.END_TXN, Errors.PRODUCER_FENCED)
            .withProbability(0.4);
        final FaultRule epoch = proxy.injectError(ApiKeys.END_TXN, Errors.INVALID_PRODUCER_EPOCH)
            .withProbability(0.3);
        // NOTE: a "transaction timeout" fault was tried here via Errors.INVALID_TXN_STATE, but that error code
        // is the wire signature of a CLIENT-side protocol violation (calling the transactional API out of
        // sequence), not a server-side timeout -- Kafka Streams doesn't treat it as retriable, so injecting it
        // just reliably crashes the instance. A real broker-side transaction timeout instead bumps the
        // producer epoch, so the client's next transactional call gets PRODUCER_FENCED/INVALID_PRODUCER_EPOCH
        // -- already covered by fence/epoch above.
        final FaultRule commitGap = proxy.disconnectOn(ApiKeys.END_TXN).withProbability(0.15);
        final FaultRule produceRetry = proxy.injectError(ApiKeys.PRODUCE, Errors.NOT_ENOUGH_REPLICAS)
            .withProbability(0.3);
        final LogCaptureAppender logs = LogCaptureAppender.createAndRegister();

        final long chaosDurationMs = Long.getLong(DURATION_PROPERTY, DEFAULT_CHAOS_DURATION_MS);
        final long deadline = chaosDurationMs > 0 ? System.currentTimeMillis() + chaosDurationMs : Long.MAX_VALUE;
        // Force real rebalances + task movement (the wire faults above mostly just retry without ever moving a
        // task between instances): alternately shed a stream thread from each instance, forcing its tasks to
        // be picked up elsewhere, then add it back so it can rejoin, before doing the same to the other
        // instance.
        runThreadShedCycles(deadline);

        // Stop the storm and the producer, then let the app fully drain and quiesce.
        proxy.clearFaults();
        producing.set(false);
        producerThread.join(Duration.ofSeconds(10).toMillis());

        final long churnLogs = logs.getMessages().stream().filter(m -> {
            final String l = m.toLowerCase(Locale.ROOT);
            return l.contains("corrupt") || l.contains("wiped") || l.contains("reviv")
                || l.contains("migrated") || l.contains("fenced");
        }).count();
        final List<LogCaptureAppender.Event> errorEvents = logs.getEvents().stream()
            .filter(e -> "ERROR".equals(e.getLevel()))
            .collect(Collectors.toList());
        final Path errorLogPath = writeErrorLog(errorEvents);
        System.out.println("CHAOS-ERROR-LOG count=" + errorEvents.size() + " path=" + errorLogPath.toAbsolutePath());
        logs.close();

        final long fired = restoreOor.timesTriggered() + fence.timesTriggered() + epoch.timesTriggered()
            + commitGap.timesTriggered() + produceRetry.timesTriggered();
        final long matched = restoreOor.timesMatched() + fence.timesMatched() + epoch.timesMatched()
            + commitGap.timesMatched() + produceRetry.timesMatched();
        final long total = produced.get();
        // Oracle 2: exactly-once. Poll the store (via IQ) until the summed window counts reach the produced
        // total, or time out. Under exactly-once this converges to EXACTLY total; > total => duplication.
        final long summed = waitForStoreSum(total);

        // matched = how many responses of each API the proxy actually saw (whether or not the probability roll
        // fired); fired = how many of those rolls hit. Logging both lets a low "fired" count on a long run be
        // told apart from "the app just didn't make many matching calls" vs. "calls happened but didn't roll".
        System.out.println("CHAOS-STATS fired=" + fired + " (oor=" + restoreOor.timesTriggered() + " fence="
            + fence.timesTriggered() + " epoch=" + epoch.timesTriggered() + " commitGap="
            + commitGap.timesTriggered() + " produce=" + produceRetry.timesTriggered() + ") matched=" + matched
            + " (oor=" + restoreOor.timesMatched() + " fence=" + fence.timesMatched() + " epoch="
            + epoch.timesMatched() + " commitGap=" + commitGap.timesMatched() + " produce="
            + produceRetry.timesMatched() + ") churnLogs=" + churnLogs + " movementCycles=" + movementCycles.get()
            + " produced=" + total + " summed=" + summed);

        // Churn sanity: the combined storm must have fired, actually churned lifecycle, AND the movement-cycle
        // mechanism itself ran, else the pass is hollow.
        assertTrue(fired > 0, "the combined storm never fired (fired=" + fired + ")");
        assertTrue(churnLogs > 0, "no corruption/migration churn observed in logs (count=" + churnLogs + ")");
        assertTrue(movementCycles.get() > 0,
            "the blackhole-driven movement cycle never ran (movementCycles=" + movementCycles.get() + ")");

        // Oracle 1: no fatal from a recoverable churn (the blackhole-driven eviction/rebalance/standby-takeover
        // above is exactly such a recoverable churn -- it must not surface as a fatal on either instance).
        checkFatal("A", uncaughtA.get());
        checkFatal("B", uncaughtB.get());
        assertEquals(total, summed,
            "exactly-once violated under combined runtime chaos: produced=" + total + " but store summed=" + summed);
        assertEquals(KafkaStreams.State.RUNNING, streamsA.state(), "instance A should be RUNNING after the storm");
        assertEquals(KafkaStreams.State.RUNNING, streamsB.state(), "instance B should be RUNNING after the storm");
    }

    private static void checkFatal(final String which, final Throwable fatal) {
        if (fatal == null) {
            return;
        }
        final String chain = throwableChain(fatal).toLowerCase(Locale.ROOT);
        if (chain.contains("committedoffset") || (chain.contains("closed") && chain.contains("segment"))) {
            fail("KAFKA-20808-class regression under combined chaos on instance " + which + ":\n"
                + throwableChain(fatal));
        }
        fail("combined runtime chaos surfaced a fatal exception on instance " + which + ":\n" + throwableChain(fatal));
    }

    /** Write every ERROR-level log event (message + full stack trace, if any) captured during the run to a file
     *  under this module's build dir, so they can be reviewed after the run without combing through gradle's
     *  full console/report output. Returns the path written to (relative to the module's working directory). */
    private Path writeErrorLog(final List<LogCaptureAppender.Event> errorEvents) throws Exception {
        final Path path = Paths.get("build", "chaos-error-log", appId + "-errors.log");
        Files.createDirectories(path.getParent());
        final StringBuilder sb = new StringBuilder();
        for (final LogCaptureAppender.Event e : errorEvents) {
            sb.append("[ERROR] ").append(e.getMessage()).append('\n');
            e.getThrowableInfo().ifPresent(t -> sb.append(t).append('\n'));
        }
        Files.write(path, sb.toString().getBytes(StandardCharsets.UTF_8));
        return path;
    }

    // --- scaffold ---

    private void startApp() throws Exception {
        streamsA = buildStreams(TestUtils.tempDirectory().getPath(), CLIENT_A, uncaughtA);
        streamsB = buildStreams(TestUtils.tempDirectory().getPath(), CLIENT_B, uncaughtB);
        streamsA.start();
        streamsB.start();
        waitForBothRunning(Duration.ofSeconds(120).toMillis());
        // Reaching RUNNING can involve a transient MissingSourceTopicException on a member's very first
        // rebalance (its local topic metadata hasn't caught up yet) that self-heals via REPLACE_THREAD -- the
        // same benign startup race already resolved in RuntimeChaosTargetedStandbyTakeoverIntegrationTest.
        // Clear it here so only fatals from the actual chaos phase below count.
        uncaughtA.set(null);
        uncaughtB.set(null);
    }

    private KafkaStreams buildStreams(final String stateDirPath,
                                       final String clientId,
                                       final AtomicReference<Throwable> fatalRef) {
        final StreamsBuilder builder = new StreamsBuilder();
        final WindowBytesStoreSupplier supplier = Stores.persistentWindowStore(
            STORE_NAME, Duration.ofMillis(RETENTION_MS), Duration.ofMillis(WINDOW_MS), false);
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMillis(WINDOW_MS), Duration.ofMillis(GRACE_MS)))
            .count(Materialized.as(supplier))
            .toStream()
            .map((kw, val) -> KeyValue.pair(kw.key(), val))
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaStreams ks = new KafkaStreams(builder.build(), props);
        ks.setUncaughtExceptionHandler(t -> {
            fatalRef.compareAndSet(null, t);
            // REPLACE_THREAD (not SHUTDOWN_CLIENT): a thread-shed-driven rebalance is a recoverable, expected
            // churn -- the instance should keep trying to rejoin, not shut down for good.
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        return ks;
    }

    private void startProducer() {
        producing.set(true);
        producerThread = new Thread(() -> {
            int i = 0;
            while (producing.get()) {
                final String key = KEYS[i % KEYS.length];
                producer.send(new ProducerRecord<>(inputTopic, null, System.currentTimeMillis(), key, "v"));
                produced.incrementAndGet();
                i++;
                try {
                    Thread.sleep(25L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            producer.flush();
        }, "chaos-producer");
        producerThread.setDaemon(true);
        producerThread.start();
    }

    /**
     * Alternately shed a stream thread from each instance ({@link KafkaStreams#removeStreamThread()}), forcing
     * its tasks to be picked up elsewhere, then add it back ({@link KafkaStreams#addStreamThread()}) so it can
     * rejoin -- before doing the same to the other instance. Graceful and in-group (no session-timeout
     * eviction), as used by {@link RuntimeChaosCleanerRaceIntegrationTest}. Runs until {@code deadlineMs}
     * ({@code Long.MAX_VALUE} for an indefinite soak).
     */
    private void runThreadShedCycles(final long deadlineMs) throws InterruptedException {
        final KafkaStreams[] instances = {streamsA, streamsB};
        int i = 0;
        while (System.currentTimeMillis() < deadlineMs) {
            final KafkaStreams victim = instances[i % instances.length];
            try {
                victim.removeStreamThread();
            } catch (final Exception ignore) {
                // shedding may race a rebalance; ignore and continue churning
            }
            movementCycles.incrementAndGet();
            Thread.sleep(MOVEMENT_WINDOW_MS);
            try {
                victim.addStreamThread();
            } catch (final Exception ignore) {
                // adding may race a rebalance; ignore
            }
            Thread.sleep(MOVEMENT_SETTLE_MS);
            i++;
        }
    }

    private void waitForBothRunning(final long timeoutMs) throws Exception {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (streamsA.state() == KafkaStreams.State.RUNNING && streamsB.state() == KafkaStreams.State.RUNNING) {
                return;
            }
            if (uncaughtA.get() != null) {
                fail("instance A died before reaching RUNNING:\n" + throwableChain(uncaughtA.get()));
            }
            if (uncaughtB.get() != null) {
                fail("instance B died before reaching RUNNING:\n" + throwableChain(uncaughtB.get()));
            }
            Thread.sleep(200);
        }
        fail("apps did not both reach RUNNING within " + timeoutMs + "ms (A=" + streamsA.state()
            + " B=" + streamsB.state() + ")");
    }

    /**
     * Sum all window counts across all keys via IQ, across BOTH instances (each task is only ever hosted
     * actively on one instance at a time). A per-instance IQ failure (rebalance/thread-replacement in
     * progress) just contributes nothing from that instance for this pass rather than aborting the whole sum.
     */
    private long storeSum() {
        long sum = 0;
        for (final KafkaStreams ks : new KafkaStreams[] {streamsA, streamsB}) {
            try {
                final ReadOnlyWindowStore<String, Long> store = ks.store(
                    StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.windowStore()));
                try (KeyValueIterator<org.apache.kafka.streams.kstream.Windowed<String>, Long> all = store.all()) {
                    while (all.hasNext()) {
                        sum += all.next().value;
                    }
                }
            } catch (final InvalidStateStoreException skip) {
                // this instance does not (yet) serve the store -- rebalance/thread-replacement in progress
            }
        }
        return sum;
    }

    private long waitForStoreSum(final long target) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(120).toMillis();
        long last = -1;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = storeSum();
                if (last >= target) {
                    return last;
                }
            } catch (final InvalidStateStoreException retry) {
                // store temporarily unavailable (rebalance/revive in progress) -- keep polling
            }
            Thread.sleep(500);
        }
        return last;
    }

    private static String throwableChain(final Throwable t) {
        final StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        while (cur != null) {
            sb.append(cur.getClass().getName()).append(": ").append(cur.getMessage()).append('\n');
            cur = cur.getCause();
        }
        return sb.toString();
    }

}