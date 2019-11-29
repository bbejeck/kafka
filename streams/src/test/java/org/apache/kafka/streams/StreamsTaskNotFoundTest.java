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

package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsTaskNotFoundTest {
    private static Logger LOG = LoggerFactory.getLogger(StreamsTaskNotFoundTest.class);

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "task_missing_test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream("input");
        stream.peek((k, v) -> System.out.println("processing key[" + k + "] value[ + " + v + "]+"))
            .groupByKey().count().toStream().to("output");

        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);

        kafkaStreams.setStateListener((newState, oldState) -> {
                LOG.info("Going from oldState " + oldState + " to newState " + newState);
            }
        );

        kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
               LOG.error("Got error!", e);
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down");
            kafkaStreams.close();
        }));

        kafkaStreams.start();
    }
}
