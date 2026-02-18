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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ValueAndHeadersTest {

    @Test
    public void shouldCreateValueAndHeaders() {
        final String value = "test-value";
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());

        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.make(value, headers);

        assertNotNull(valueAndHeaders);
        assertEquals(value, valueAndHeaders.value());
        assertEquals(headers, valueAndHeaders.headers());
    }

    @Test
    public void shouldReturnNullForNullValue() {
        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.make(null, new RecordHeaders());
        assertNull(valueAndHeaders);
    }

    @Test
    public void shouldCreateWithNullHeaders() {
        final String value = "test-value";
        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.make(value, null);

        assertNotNull(valueAndHeaders);
        assertEquals(value, valueAndHeaders.value());
        assertNotNull(valueAndHeaders.headers());
        assertEquals(0, valueAndHeaders.headers().toArray().length);
    }

    @Test
    public void shouldAllowNullableValue() {
        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.makeAllowNullable(null, new RecordHeaders());

        assertNotNull(valueAndHeaders);
        assertNull(valueAndHeaders.value());
    }

    @Test
    public void shouldGetValueOrNull() {
        final String value = "test-value";
        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.make(value, new RecordHeaders());

        assertEquals(value, ValueAndHeaders.getValueOrNull(valueAndHeaders));
        assertNull(ValueAndHeaders.getValueOrNull(null));
    }

    @Test
    public void shouldImplementEquals() {
        final String value = "test-value";
        final Headers headers1 = new RecordHeaders();
        headers1.add("key1", "value1".getBytes());

        final Headers headers2 = new RecordHeaders();
        headers2.add("key1", "value1".getBytes());

        final ValueAndHeaders<String> valueAndHeaders1 = ValueAndHeaders.make(value, headers1);
        final ValueAndHeaders<String> valueAndHeaders2 = ValueAndHeaders.make(value, headers2);

        assertEquals(valueAndHeaders1, valueAndHeaders2);
        assertEquals(valueAndHeaders1.hashCode(), valueAndHeaders2.hashCode());
    }

    @Test
    public void shouldNotBeEqualWithDifferentValues() {
        final Headers headers = new RecordHeaders();

        final ValueAndHeaders<String> valueAndHeaders1 = ValueAndHeaders.make("value1", headers);
        final ValueAndHeaders<String> valueAndHeaders2 = ValueAndHeaders.make("value2", headers);

        assertNotEquals(valueAndHeaders1, valueAndHeaders2);
    }

    @Test
    public void shouldNotBeEqualWithDifferentHeaders() {
        final String value = "test-value";

        final Headers headers1 = new RecordHeaders();
        headers1.add("key1", "value1".getBytes());

        final Headers headers2 = new RecordHeaders();
        headers2.add("key2", "value2".getBytes());

        final ValueAndHeaders<String> valueAndHeaders1 = ValueAndHeaders.make(value, headers1);
        final ValueAndHeaders<String> valueAndHeaders2 = ValueAndHeaders.make(value, headers2);

        assertNotEquals(valueAndHeaders1, valueAndHeaders2);
    }

    @Test
    public void shouldImplementToString() {
        final String value = "test-value";
        final Headers headers = new RecordHeaders();
        headers.add("key1", "value1".getBytes());

        final ValueAndHeaders<String> valueAndHeaders = ValueAndHeaders.make(value, headers);
        final String toString = valueAndHeaders.toString();

        assertNotNull(toString);
        assertEquals(true, toString.contains("value=test-value"));
        assertEquals(true, toString.contains("headers="));
    }
}