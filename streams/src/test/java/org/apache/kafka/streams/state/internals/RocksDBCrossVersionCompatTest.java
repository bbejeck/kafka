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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Guards that a {@link RocksDBStore} on-disk directory written by one run reopens and reads back
 * correctly — both the value column family and the KIP-1035 committed-offset column family.
 *
 * <p>The fixture is built <em>in-process</em> by {@link #writeFixture(File)}, so this test carries no
 * committed binary artifact. Because a JVM loads exactly one {@code rocksdbjni} native library, the
 * write and read here use the <em>same</em> RocksDB version: this is a same-version durability/reopen
 * guard, not a true cross-version test on its own.
 *
 * <p>To exercise a genuine <em>old&nbsp;&rarr;&nbsp;new</em> upgrade, point the test at a fixture that a
 * different RocksDB version already produced by setting {@code -Drocksdb.fixture.dir=/path/to/stateDir};
 * the write step is then skipped and only the reopen/read assertions run. {@link #writeFixture(File)} is
 * the single shared definition of the fixture, so an external process running an older {@code rocksdbjni}
 * can produce a directory this test then reads. See the class Javadoc note in the PR for the harness.
 */
public class RocksDBCrossVersionCompatTest {

    private static final String STORE_NAME = "db-name";
    private static final String METRICS_SCOPE = "metrics-scope";
    private static final Map<String, String> EXPECTED_VALUES = new LinkedHashMap<>();
    private static final Map<TopicPartition, Long> EXPECTED_OFFSETS = new LinkedHashMap<>();

    static {
        EXPECTED_VALUES.put("k1", "v1");
        EXPECTED_VALUES.put("k2", "v2");
        EXPECTED_VALUES.put("k3", "v3");
        EXPECTED_OFFSETS.put(new TopicPartition("topic-0", 0), 100L);
        EXPECTED_OFFSETS.put(new TopicPartition("topic-1", 0), 200L);
    }

    /**
     * Writes the canonical fixture into {@code stateDir}: three key-values plus the KIP-1035 committed
     * offsets. This is the one authoritative definition of the fixture — invoke it from an older
     * {@code rocksdbjni} process to produce an old-version directory for the cross-version harness.
     */
    static void writeFixture(final File stateDir) {
        final RocksDBStore store = openStore(stateDir);
        try {
            EXPECTED_VALUES.forEach((k, v) ->
                store.put(new Bytes(k.getBytes(UTF_8)), v.getBytes(UTF_8)));
            store.commit(EXPECTED_OFFSETS);
        } finally {
            store.close();
        }
    }

    private static RocksDBStore openStore(final File stateDir) {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        final InternalMockProcessorContext<?, ?> context = new InternalMockProcessorContext<>(
            stateDir,
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(props)
        );
        final RocksDBStore store = new RocksDBStore(STORE_NAME, METRICS_SCOPE);
        store.init(context, store);
        return store;
    }

    @Test
    public void shouldReopenAndReadStoreWrittenByRocksDB() {
        // Either read a fixture produced by a different RocksDB version (cross-version harness) or,
        // by default, build one in-process (same-version durability guard). Same assertions either way.
        final String external = System.getProperty("rocksdb.fixture.dir");
        final File stateDir = external != null ? new File(external) : TestUtils.tempDirectory();
        if (external == null) {
            writeFixture(stateDir);
        }

        final RocksDBStore store = openStore(stateDir);
        try {
            EXPECTED_VALUES.forEach((k, v) ->
                assertArrayEquals(
                    v.getBytes(UTF_8),
                    store.get(new Bytes(k.getBytes(UTF_8))),
                    "value for " + k + " must survive reopen"));
            EXPECTED_OFFSETS.forEach((tp, offset) ->
                assertEquals(
                    offset,
                    store.committedOffset(tp),
                    "KIP-1035 committed offset for " + tp + " must survive reopen"));
        } finally {
            store.close();
        }
    }
}
