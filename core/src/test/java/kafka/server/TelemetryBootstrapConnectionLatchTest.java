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
package kafka.server;

import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.metrics.ClientMetricsConfigs;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;

import org.junit.jupiter.api.extension.ExtendWith;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration repro for INC-12544 (active connections increase after upgrading the Java client
 * to 3.9.2), exercising the real {@code KafkaConsumer} -> {@code NetworkClient} ->
 * {@code TelemetrySender} stack against a real broker.
 *
 * <p>Nikit's theory: KIP-714 telemetry can latch its sticky node onto the client's bootstrap
 * connection and then keep it alive with periodic pushes, so the client permanently holds one
 * extra connection. The unit-level proof lives in {@code NetworkClientTest}; this test verifies
 * the same defect end to end, with real sockets and real idle handling, and turns the "phantom
 * connection" into a concrete count.
 *
 * <p><b>How the startup race is forced deterministically.</b> The latch is only possible in the
 * narrow window before the first {@code MetadataResponse} is processed (afterwards
 * {@code fetchNodes()} contains only real broker ids and {@code leastLoadedNode} can no longer
 * return the negative-id bootstrap node). We put a tiny TCP proxy in front of the broker and
 * point the consumer's {@code bootstrap.servers} at the proxy. The proxy lets the first
 * server->client frame through (the {@code ApiVersionsResponse}, so the bootstrap connection
 * becomes ready) but holds every later frame for a short window, including the
 * {@code MetadataResponse}. During the hold telemetry fires, sees only the bootstrap node in
 * metadata, and latches onto the bootstrap (proxy) connection.
 *
 * <p><b>Why the proxy's connection count is the signal.</b> Metadata returns the broker's real
 * advertised address, so all real work (fetch, coordinator, commit) goes <i>directly</i> to the
 * broker, never through the proxy. The only connection the proxy ever carries is the bootstrap
 * connection. We set the client's {@code connections.max.idle.ms} short (4s) and the telemetry
 * push interval shorter (500ms): if telemetry latched the bootstrap connection it keeps writing
 * to it every 500ms and the connection survives; otherwise nothing writes to it and the client
 * reaps it after 4s. Asserting that the proxy still has a live connection after ~9s therefore
 * demonstrates telemetry is holding the extra connection open.
 *
 * <p>On a client with KAFKA-20393 (4.2.1+) the sticky node would be cleared once real metadata
 * arrives, so the bootstrap connection would get no telemetry traffic and idle out -> the proxy
 * count would drop to 0 and this assertion would fail. This test is written against the 3.9.2
 * (pre-fix) client and asserts the buggy outcome.
 */
@ExtendWith(value = ClusterTestExtensions.class)
public class TelemetryBootstrapConnectionLatchTest {

    private static final String TOPIC = "telemetry-latch-topic";
    private static final int CLIENT_IDLE_MS = 4000;
    private static final int PUSH_INTERVAL_MS = 500;
    private static final long METADATA_HOLD_MS = 1200;
    private static final long RUN_MS = 9000;

    private final ClusterInstance cluster;

    public TelemetryBootstrapConnectionLatchTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(types = {Type.KRAFT}, brokers = 1, serverProperties = {
        @ClusterConfigProperty(key = "metric.reporters",
            value = "kafka.server.TelemetryBootstrapConnectionLatchTest$PushCountingReporter")
    })
    public void telemetryLatchesAndHoldsBootstrapConnection() throws Exception {
        PushCountingReporter.PUSH_COUNT.set(0);
        createShortIntervalClientMetricsSubscription();
        createTopicAndProduce();

        InetSocketAddress broker = parseFirst(cluster.bootstrapServers());
        try (FrameHoldingProxy proxy = FrameHoldingProxy.start(broker, METADATA_HOLD_MS)) {
            String proxyBootstrap = "127.0.0.1:" + proxy.localPort();

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyBootstrap);
            consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "telemetry-latch-consumer");
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, true);
            // Short idle so that, without telemetry keepalive, the bootstrap connection would be reaped.
            consumerProps.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, CLIENT_IDLE_MS);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            int liveAtEnd;
            int consumed = 0;
            int brokerPushes;
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                // Use manual assignment (not subscribe) so there is no group rebalance to wait on;
                // the consumer fetches directly from the broker while telemetry runs in the background.
                consumer.assign(Collections.singletonList(new TopicPartition(TOPIC, 0)));

                long deadline = System.currentTimeMillis() + RUN_MS;
                while (System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                    consumed += records.count();
                }

                brokerPushes = PushCountingReporter.PUSH_COUNT.get();
                liveAtEnd = proxy.liveConnections();
                System.out.printf(
                    "[telemetry-latch] consumed=%d assignment=%s brokerPushes=%d proxy: peakLive=%d liveAtEnd=%d totalAccepted=%d framesHeld=%d%n",
                    consumed, consumer.assignment(), brokerPushes,
                    proxy.peakLiveConnections(), liveAtEnd, proxy.totalAccepted(), proxy.framesHeld());
            }

            assertTrue(proxy.totalAccepted() >= 1,
                "consumer should have bootstrapped through the proxy at least once");
            assertTrue(consumed > 0,
                "consumer should have consumed the produced records (real work happens directly "
                    + "against the broker, not through the proxy). consumed=" + consumed);
            assertTrue(brokerPushes > 0,
                "telemetry must actually be pushing for this test to be meaningful. brokerPushes=" + brokerPushes);
            // The run is well past the client idle timeout (4s). The only traffic on the bootstrap
            // (proxy) connection after startup is the periodic telemetry push, so the only thing
            // that can keep it alive past the idle timeout is the latched telemetry sticky node ->
            // on 3.9.2 this is the extra connection. On a KAFKA-20393-fixed client the sticky node
            // would be cleared once real metadata arrived and this would be 0.
            assertTrue(liveAtEnd >= 1,
                "telemetry should be holding the bootstrap connection open past the idle timeout "
                    + "(extra connection). liveAtEnd=" + liveAtEnd + ". On a KAFKA-20393-fixed client "
                    + "this would be 0.");
        }
    }

    private void createShortIntervalClientMetricsSubscription() throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "inc12544-sub");
            List<AlterConfigOp> ops = List.of(
                new AlterConfigOp(new ConfigEntry(ClientMetricsConfigs.PUSH_INTERVAL_MS,
                    Integer.toString(PUSH_INTERVAL_MS)), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(ClientMetricsConfigs.SUBSCRIPTION_METRICS,
                    "org.apache.kafka"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(ClientMetricsConfigs.CLIENT_MATCH_PATTERN,
                    ClientMetricsConfigs.CLIENT_SOFTWARE_NAME + "=apache-kafka-java"), AlterConfigOp.OpType.SET));
            admin.incrementalAlterConfigs(Collections.singletonMap(resource, ops)).all().get();
        }
    }

    private void createTopicAndProduce() throws ExecutionException, InterruptedException {
        try (Admin admin = cluster.createAdminClient()) {
            admin.createTopics(Collections.singletonList(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
        cluster.waitForTopic(TOPIC, 1);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < 20; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "k" + i, "v" + i));
            }
            producer.flush();
        }
    }

    private static InetSocketAddress parseFirst(String bootstrapServers) {
        String first = bootstrapServers.split(",")[0].trim();
        int idx = first.lastIndexOf(':');
        String host = first.substring(0, idx);
        int port = Integer.parseInt(first.substring(idx + 1));
        return new InetSocketAddress(host, port);
    }

    /** Broker-side reporter that counts PushTelemetry payloads, to confirm telemetry is flowing. */
    public static final class PushCountingReporter implements ClientTelemetry, MetricsReporter {
        static final AtomicInteger PUSH_COUNT = new AtomicInteger();

        @Override
        public ClientTelemetryReceiver clientReceiver() {
            return (AuthorizableRequestContext context, ClientTelemetryPayload payload) -> PUSH_COUNT.incrementAndGet();
        }

        @Override
        public void init(List<KafkaMetric> metrics) {
        }

        @Override
        public void metricChange(KafkaMetric metric) {
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }
    }

    /**
     * A minimal TCP proxy that forwards to {@code target}, counts live connections, and holds back
     * every server->client frame after the first one for {@code holdMs} after the connection opens
     * (Kafka wire frames are length-prefixed with a 4-byte big-endian size). Holding the frames
     * after the ApiVersionsResponse keeps the client from processing its MetadataResponse during
     * the startup window, forcing telemetry to latch the bootstrap connection.
     */
    static final class FrameHoldingProxy implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final InetSocketAddress target;
        private final long holdMs;
        private final AtomicInteger live = new AtomicInteger();
        private final AtomicInteger peakLive = new AtomicInteger();
        private final AtomicInteger totalAccepted = new AtomicInteger();
        private final AtomicInteger framesHeld = new AtomicInteger();
        private final List<Socket> sockets = new CopyOnWriteArrayList<>();
        private volatile boolean closed;
        private final Thread acceptThread;

        private FrameHoldingProxy(ServerSocket serverSocket, InetSocketAddress target, long holdMs) {
            this.serverSocket = serverSocket;
            this.target = target;
            this.holdMs = holdMs;
            this.acceptThread = new Thread(this::acceptLoop, "telemetry-latch-proxy-accept");
            this.acceptThread.setDaemon(true);
            this.acceptThread.start();
        }

        static FrameHoldingProxy start(InetSocketAddress target, long holdMs) throws IOException {
            ServerSocket ss = new ServerSocket();
            ss.bind(new InetSocketAddress("127.0.0.1", 0));
            return new FrameHoldingProxy(ss, target, holdMs);
        }

        int localPort() {
            return serverSocket.getLocalPort();
        }

        int liveConnections() {
            return live.get();
        }

        int peakLiveConnections() {
            return peakLive.get();
        }

        int totalAccepted() {
            return totalAccepted.get();
        }

        int framesHeld() {
            return framesHeld.get();
        }

        private void acceptLoop() {
            while (!closed) {
                try {
                    Socket client = serverSocket.accept();
                    Socket broker = new Socket();
                    broker.connect(target);
                    sockets.add(client);
                    sockets.add(broker);
                    totalAccepted.incrementAndGet();
                    int now = live.incrementAndGet();
                    peakLive.accumulateAndGet(now, Math::max);
                    long openedAt = System.currentTimeMillis();

                    // Decrement the live count exactly once, whichever direction closes first.
                    AtomicBoolean decremented = new AtomicBoolean();
                    Runnable onClose = () -> {
                        closeQuietly(client);
                        closeQuietly(broker);
                        if (decremented.compareAndSet(false, true)) {
                            live.updateAndGet(v -> Math.max(0, v - 1));
                        }
                    };

                    Thread c2b = new Thread(() -> pumpRaw(client, broker, onClose), "telemetry-latch-c2b");
                    Thread b2c = new Thread(() -> pumpFramesWithHold(broker, client, openedAt, onClose), "telemetry-latch-b2c");
                    c2b.setDaemon(true);
                    b2c.setDaemon(true);
                    c2b.start();
                    b2c.start();
                } catch (IOException e) {
                    if (!closed) {
                        // transient accept error; keep serving
                        continue;
                    }
                    return;
                }
            }
        }

        private void pumpRaw(Socket from, Socket to, Runnable onClose) {
            try {
                InputStream in = from.getInputStream();
                OutputStream out = to.getOutputStream();
                byte[] buf = new byte[16 * 1024];
                int n;
                while ((n = in.read(buf)) != -1) {
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException ignored) {
                // connection closed
            } finally {
                onClose.run();
            }
        }

        private void pumpFramesWithHold(Socket from, Socket to, long openedAt, Runnable onClose) {
            try {
                DataInputStream in = new DataInputStream(from.getInputStream());
                DataOutputStream out = new DataOutputStream(to.getOutputStream());
                int frameIndex = 0;
                while (true) {
                    int len = in.readInt();
                    byte[] payload = new byte[len];
                    in.readFully(payload);

                    // Let the first frame (ApiVersionsResponse) through so the bootstrap connection
                    // becomes ready; hold everything after it (incl. MetadataResponse) during the
                    // startup window so telemetry latches the bootstrap connection.
                    if (frameIndex >= 1) {
                        long remaining = (openedAt + holdMs) - System.currentTimeMillis();
                        if (remaining > 0) {
                            framesHeld.incrementAndGet();
                            Thread.sleep(remaining);
                        }
                    }

                    out.writeInt(len);
                    out.write(payload);
                    out.flush();
                    frameIndex++;
                }
            } catch (IOException | InterruptedException ignored) {
                // connection closed or interrupted
            } finally {
                onClose.run();
            }
        }

        private static void closeQuietly(Socket s) {
            try {
                if (!s.isClosed()) {
                    s.close();
                }
            } catch (IOException ignored) {
                // ignore
            }
        }

        @Override
        public void close() {
            closed = true;
            closeQuietly0(serverSocket);
            for (Socket s : sockets) {
                closeQuietly(s);
            }
        }

        private static void closeQuietly0(ServerSocket s) {
            try {
                s.close();
            } catch (IOException ignored) {
                // ignore
            }
        }
    }
}