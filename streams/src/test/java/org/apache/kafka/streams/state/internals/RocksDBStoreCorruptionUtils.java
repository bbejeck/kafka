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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test utility for directly manipulating RocksDB column family state to simulate
 * store corruption scenarios (e.g., unclean shutdown).
 */
public final class RocksDBStoreCorruptionUtils {

    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final byte[] OFFSETS_COLUMN_FAMILY_NAME = "offsets".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STATUS_KEY = STRING_SERIALIZER.serialize(null, "status");
    private static final byte[] POSITION_KEY = STRING_SERIALIZER.serialize(null, "position");
    private static final byte[] OPEN_STATE = Serdes.Long().serializer().serialize(null, 1L);

    private RocksDBStoreCorruptionUtils() {
    }

    /**
     * Reads the committed changelog offsets from the store's offsets column family, keyed by the
     * {@code TopicPartition.toString()} form used by the store. Used to capture the real offsets before
     * simulating a pre-KIP-1035 layout (offsets in a legacy checkpoint file rather than the offsets CF).
     *
     * @param dbDir the RocksDB store directory
     */
    public static java.util.Map<String, Long> readCommittedOffsets(final File dbDir) throws RocksDBException {
        final java.util.Map<String, Long> offsets = new java.util.HashMap<>();
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);
                try (final org.rocksdb.RocksIterator iter = db.newIterator(offsetsCf)) {
                    iter.seekToFirst();
                    while (iter.isValid()) {
                        final byte[] key = iter.key();
                        if (!Arrays.equals(key, STATUS_KEY) && !Arrays.equals(key, POSITION_KEY)) {
                            offsets.put(new String(key, StandardCharsets.UTF_8),
                                Serdes.Long().deserializer().deserialize(null, iter.value()));
                        }
                        iter.next();
                    }
                }
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
        return offsets;
    }

    /**
     * Reads the store status marker from the offsets column family: {@code 1L} == open, {@code 0L} == closed,
     * or {@code null} if the status key is absent (store never opened). Used to assert the on-disk open/closed
     * state after a (possibly failed) close -- e.g. to confirm that a clean shutdown always leaves the store
     * marked closed, and that a corruption setup ({@link #setStoreStatusToOpen}) actually took effect.
     *
     * @param dbDir the RocksDB store directory
     */
    public static Long readStatus(final File dbDir) throws RocksDBException {
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);
                final byte[] valueBytes = db.get(offsetsCf, STATUS_KEY);
                if (valueBytes == null) {
                    return null;
                }
                return Serdes.Long().deserializer().deserialize(null, valueBytes);
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    /**
     * Overwrites the committed changelog offset for a single changelog partition in the offsets column
     * family. The {@code topicPartition} key must be the {@code TopicPartition.toString()} form used by
     * the store (the same form returned by {@link #readCommittedOffsets(File)}). Used to simulate an
     * on-disk committed offset that has drifted ahead of (or behind) the actual changelog contents --
     * e.g. an offset left ahead of the changelog log-end by a partial/failed commit.
     *
     * @param dbDir          the RocksDB store directory
     * @param topicPartition the changelog partition key, in {@code TopicPartition.toString()} form
     * @param offset         the committed offset value to write
     */
    public static void writeCommittedOffset(final File dbDir,
                                            final String topicPartition,
                                            final long offset) throws RocksDBException {
        final byte[] key = STRING_SERIALIZER.serialize(null, topicPartition);
        final byte[] value = Serdes.Long().serializer().serialize(null, offset);
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);
                db.put(offsetsCf, key, value);
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    /**
     * Overwrites the store status key to 1L (open), simulating an unclean shutdown.
     *
     * @param dbDir the RocksDB store directory
     */
    public static void setStoreStatusToOpen(final File dbDir) throws RocksDBException {
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);
                db.put(offsetsCf, STATUS_KEY, OPEN_STATE);
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    /**
     * Overwrites a single changelog partition's committed-offset value with a wrong-length (4-byte)
     * blob, so that the {@code Long} deserializer fails deterministically when the store reads the
     * committed offset on reopen. The {@code topicPartition} key must be the
     * {@code TopicPartition.toString()} form used by the store (as returned by
     * {@link #readCommittedOffsets(File)}). Used to simulate an on-disk corrupt/torn committed offset.
     *
     * @param dbDir          the RocksDB store directory
     * @param topicPartition the changelog partition key, in {@code TopicPartition.toString()} form
     */
    public static void corruptOffset(final File dbDir, final String topicPartition) throws RocksDBException {
        final byte[] key = STRING_SERIALIZER.serialize(null, topicPartition);
        // A Long is 8 bytes; a 4-byte value makes LongDeserializer throw a SerializationException.
        final byte[] garbage = new byte[] {0x00, 0x00, 0x00, 0x01};
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);
                db.put(offsetsCf, key, garbage);
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    /**
     * Overwrites the store's position key with a single byte carrying an unknown serialization version,
     * so that {@code PositionSerde.deserialize} fails deterministically when the store is reopened.
     * Used to simulate an on-disk corrupt/garbage IQ position vector.
     *
     * @param dbDir the RocksDB store directory
     */
    public static void corruptPosition(final File dbDir) throws RocksDBException {
        // Version byte 0x7F is not a recognized Position serialization version, so deserialize throws.
        final byte[] garbage = new byte[] {(byte) 0x7F};
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);
                db.put(offsetsCf, POSITION_KEY, garbage);
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    /**
     * Deletes all offset entries from the offsets column family, keeping only the status key.
     *
     * @param dbDir the RocksDB store directory
     */
    public static void deleteOffsets(final File dbDir) throws RocksDBException {
        try (final DBOptions dbOptions = new DBOptions();
             final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {

            final List<ColumnFamilyDescriptor> cfDescriptors = listCfDescriptors(dbDir, cfOptions);
            final List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            try (final RocksDB db = RocksDB.open(dbOptions, dbDir.getAbsolutePath(), cfDescriptors, cfHandles)) {
                final ColumnFamilyHandle offsetsCf = findOffsetsCf(cfHandles, cfDescriptors);

                try (final org.rocksdb.RocksIterator iter = db.newIterator(offsetsCf)) {
                    iter.seekToFirst();
                    while (iter.isValid()) {
                        final byte[] key = iter.key();
                        if (!Arrays.equals(key, STATUS_KEY)) {
                            db.delete(offsetsCf, key);
                        }
                        iter.next();
                    }
                }
            } finally {
                cfHandles.forEach(ColumnFamilyHandle::close);
            }
        }
    }

    private static List<ColumnFamilyDescriptor> listCfDescriptors(final File dbDir,
                                                                   final ColumnFamilyOptions cfOptions) throws RocksDBException {
        return RocksDB.listColumnFamilies(new Options(), dbDir.getAbsolutePath())
            .stream()
            .map(name -> new ColumnFamilyDescriptor(name, cfOptions))
            .collect(Collectors.toList());
    }

    private static ColumnFamilyHandle findOffsetsCf(final List<ColumnFamilyHandle> handles,
                                                     final List<ColumnFamilyDescriptor> descriptors) {
        for (int i = 0; i < descriptors.size(); i++) {
            if (Arrays.equals(descriptors.get(i).getName(), OFFSETS_COLUMN_FAMILY_NAME)) {
                return handles.get(i);
            }
        }
        throw new IllegalStateException("Offsets column family not found in RocksDB store");
    }
}
