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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.ValueAndHeaders;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ValueAndHeadersSerializerTest {

    private final Serializer<String> stringSerializer = Serdes.String().serializer();
    private final ValueAndHeadersSerializer<String> serializer = new ValueAndHeadersSerializer<>(stringSerializer);

    @Test
    public void shouldSerializeNullAsNull() {
        final byte[] result = serializer.serialize("topic", null);
        assertNull(result);
    }

    @Test
    public void shouldSerializeValueWithEmptyHeaders() {
        final String value = "test-value";
        final Headers headers = new RecordHeaders();
        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.make(value, headers);

        final byte[] result = serializer.serialize("topic", valueAndHeaders);

        assertNotNull(result);
        assertTrue(result.length > 0);
    }

    @Test
    public void shouldSerializeValueWithNullHeaders() {
        final String value = "test-value";
        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.make(value, null);

        final byte[] result = serializer.serialize("topic", valueAndHeaders);

        assertNotNull(result);
        assertTrue(result.length > 0);
    }

    @Test
    public void shouldSerializeValueWithHeaders() {
        final String value = "test-value";
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        headers.add("key2", "value2".getBytes());
        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.make(value, headers);

        final byte[] result = serializer.serialize("topic", valueAndHeaders);

        assertNotNull(result);
        assertTrue(result.length > 0);
    }

    @Test
    public void shouldSerializeAndDeserializeConsistently() {
        final String value = "test-value";
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        headers.add("key2", null);
        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.make(value, headers);

        final byte[] serialized = serializer.serialize("topic", valueAndHeaders);
        final ValueAndHeadersDeserializer<String> deserializer = new ValueAndHeadersDeserializer<>(Serdes.String().deserializer());
        final ValueAndHeaders<String> deserialized = deserializer.deserialize("topic", serialized);

        assertNotNull(deserialized);
        assertArrayEquals(value.getBytes(), deserialized.value().getBytes());
        assertNotNull(deserialized.headers());
    }

    @Test
    public void shouldHandleNullValueInValueAndHeaders() {
        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.makeAllowNullable(null, new RecordHeaders());
        final byte[] result = serializer.serialize("topic", valueAndHeaders);
        assertNull(result);
    }
}