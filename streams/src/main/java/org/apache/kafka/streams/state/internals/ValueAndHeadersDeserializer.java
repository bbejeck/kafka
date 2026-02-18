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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.ValueAndHeaders;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;

/**
 * Deserializer for ValueAndHeaders.
 * Deserialization format (per KIP-1271):
 * [headersSize(varint)][headersBytes][value]
 * <p>
 * Where:
 * - headersSize: Size of the headersBytes section in bytes, encoded as varint
 * - headersBytes:
 *   - For null/empty headers: headersSize = 0, headersBytes is omitted (0 bytes)
 *   - For non-empty headers: headersSize > 0, serialized headers in the format [count(varint)][header1][header2]... to be processed by HeadersDeserializer.
 * - value: Serialized value to be deserialized with the provided value deserializer
 * <p>
 * This is used by KIP-1271 to deserialize values with headers from session state stores.
 */
class ValueAndHeadersDeserializer<V> implements WrappingNullableDeserializer<ValueAndHeaders<V>, Void, V> {
    private static final HeadersDeserializer HEADERS_DESERIALIZER = new HeadersDeserializer();

    public final Deserializer<V> valueDeserializer;
    private final HeadersDeserializer headersDeserializer;

    ValueAndHeadersDeserializer(final Deserializer<V> valueDeserializer) {
        Objects.requireNonNull(valueDeserializer);
        this.valueDeserializer = valueDeserializer;
        this.headersDeserializer = new HeadersDeserializer();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        valueDeserializer.configure(configs, isKey);
        headersDeserializer.configure(configs, isKey);
    }

    @Override
    public ValueAndHeaders<V> deserialize(final String topic, final byte[] valueAndHeaders) {
        if (valueAndHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(valueAndHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);

        final byte[] rawHeaders = readBytes(buffer, headersSize);
        final Headers headers = headersDeserializer.deserialize(topic, rawHeaders);
        final byte[] rawValue = readBytes(buffer, buffer.remaining());
        final V value = valueDeserializer.deserialize(topic, headers, rawValue);

        return ValueAndHeaders.make(value, headers);
    }

    @Override
    public void close() {
        valueDeserializer.close();
        headersDeserializer.close();
    }

    @Override
    public void setIfUnset(final SerdeGetter getter) {
        // ValueAndHeadersDeserializer never wraps a null deserializer (or configure would throw),
        // but it may wrap a deserializer that itself wraps a null deserializer.
        initNullableDeserializer(valueDeserializer, getter);
    }

    /**
     * Reads the specified number of bytes from the buffer with validation.
     *
     * @param buffer the ByteBuffer to read from
     * @param length the number of bytes to read
     * @return the byte array containing the read bytes
     * @throws SerializationException if buffer doesn't have enough bytes
     */
    private static byte[] readBytes(final ByteBuffer buffer, final int length) {
        if (buffer.remaining() < length) {
            throw new SerializationException(
                "Invalid ValueAndHeaders format: expected " + length +
                " bytes but only " + buffer.remaining() + " bytes remaining"
            );
        }
        final byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * Extract value from serialized ValueAndHeaders.
     */
    static <T> T value(final byte[] rawValueAndHeaders, final Deserializer<T> deserializer) {
        if (rawValueAndHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueAndHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        // skip headers
        buffer.position(buffer.position() + headersSize);
        final byte[] bytes = readBytes(buffer, buffer.remaining());

        return deserializer.deserialize("", bytes);
    }

    /**
     * Extract headers from serialized ValueAndHeaders.
     */
    static Headers headers(final byte[] rawValueAndHeaders) {
        if (rawValueAndHeaders == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(rawValueAndHeaders);
        final int headersSize = ByteUtils.readVarint(buffer);
        final byte[] rawHeaders = readBytes(buffer, headersSize);
        return HEADERS_DESERIALIZER.deserialize("", rawHeaders);
    }
}
