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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.ValueAndHeaders;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ValueAndHeadersDeserializerTest {

    private final ValueAndHeadersSerializer<String> serializer = new ValueAndHeadersSerializer<>(Serdes.String().serializer());
    private final ValueAndHeadersDeserializer<String> deserializer = new ValueAndHeadersDeserializer<>(Serdes.String().deserializer());

    @Test
    public void shouldDeserializeNullAsNull() {
        final ValueAndHeaders<String> result = deserializer.deserialize("topic", null);
        assertNull(result);
    }

    @Test
    public void shouldDeserializeValueWithEmptyHeaders() {
        final String value = "test-value";
        final Headers headers = new RecordHeaders();
        final ValueAndHeaders<String> original = ValueAndHeaders.make(value, headers);
        final byte[] serialized = serializer.serialize("topic", original);

        final ValueAndHeaders<String> result = deserializer.deserialize("topic", serialized);

        assertNotNull(result);
        assertEquals(value, result.value());
        assertNotNull(result.headers());
        assertEquals(0, result.headers().toArray().length);
    }

    @Test
    public void shouldDeserializeValueWithHeaders() {
        final String value = "test-value";
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        headers.add("key2", "value2".getBytes());
        final ValueAndHeaders<String> original = ValueAndHeaders.make(value, headers);
        final byte[] serialized = serializer.serialize("topic", original);

        final ValueAndHeaders<String> result = deserializer.deserialize("topic", serialized);

        assertNotNull(result);
        assertEquals(value, result.value());
        assertNotNull(result.headers());
        assertEquals(2, result.headers().toArray().length);

        final Header[] headersArray = result.headers().toArray();
        assertEquals("key1", headersArray[0].key());
        assertArrayEquals("value1".getBytes(), headersArray[0].value());
        assertEquals("key2", headersArray[1].key());
        assertArrayEquals("value2".getBytes(), headersArray[1].value());
    }

    @Test
    public void shouldDeserializeValueWithNullHeaderValue() {
        final String value = "test-value";
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        headers.add("key2", null);
        final ValueAndHeaders<String> original = ValueAndHeaders.make(value, headers);
        final byte[] serialized = serializer.serialize("topic", original);

        final ValueAndHeaders<String> result = deserializer.deserialize("topic", serialized);

        assertNotNull(result);
        assertEquals(value, result.value());
        assertNotNull(result.headers());
        assertEquals(2, result.headers().toArray().length);

        final Header[] headersArray = result.headers().toArray();
        assertEquals("key1", headersArray[0].key());
        assertArrayEquals("value1".getBytes(), headersArray[0].value());
        assertEquals("key2", headersArray[1].key());
        assertNull(headersArray[1].value());
    }

    @Test
    public void shouldExtractValueFromSerializedData() {
        final String expectedValue = "test-value";
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        final ValueAndHeaders<String> original = ValueAndHeaders.make(expectedValue, headers);
        final byte[] serialized = serializer.serialize("topic", original);

        final String extractedValue = ValueAndHeadersDeserializer.value(serialized, Serdes.String().deserializer());

        assertEquals(expectedValue, extractedValue);
    }

    @Test
    public void shouldExtractHeadersFromSerializedData() {
        final String value = "test-value";
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());
        headers.add("key2", "value2".getBytes());
        final ValueAndHeaders<String> original = ValueAndHeaders.make(value, headers);
        final byte[] serialized = serializer.serialize("topic", original);

        final Headers extractedHeaders = ValueAndHeadersDeserializer.headers(serialized);

        assertNotNull(extractedHeaders);
        assertEquals(2, extractedHeaders.toArray().length);
    }

    @Test
    public void shouldHandleUtf8Headers() {
        final String value = "test-value";
        final Headers headers = new RecordHeaders();
        headers.add("emoji-key", "🎉".getBytes(StandardCharsets.UTF_8));
        final ValueAndHeaders<String> original = ValueAndHeaders.make(value, headers);
        final byte[] serialized = serializer.serialize("topic", original);

        final ValueAndHeaders<String> result = deserializer.deserialize("topic", serialized);

        assertNotNull(result);
        final Header[] headersArray = result.headers().toArray();
        assertEquals("emoji-key", headersArray[0].key());
        assertArrayEquals("🎉".getBytes(StandardCharsets.UTF_8), headersArray[0].value());
    }
}
