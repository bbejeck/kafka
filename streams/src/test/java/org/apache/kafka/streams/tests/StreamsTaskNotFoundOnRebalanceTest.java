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

package org.apache.kafka.streams.tests;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.ThreadMetadata;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class StreamsTaskNotFoundOnRebalanceTest {

    public static void main(final String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("StreamsTaskNotFoundOnRebalanceTest are expecting parameters: " +
                "propFile but got none");
            System.exit(1);
        }

        System.out.println("StreamsTest instance started");

        final String propFileName = args[0];

        final Properties streamsProperties = Utils.loadProps(propFileName);
        final String kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (kafka == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            System.exit(1);
        }
        
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "tasks-not-found-on-rebalance");;
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();
        final ValueMapper<Long, String> countMapper = Object::toString;
        final Random random = new Random();

        final KStream<String, String> inputStream = builder.stream("input", Consumed.with(stringSerde, stringSerde));
        final int reportInterval = 5000;
        inputStream.peek(
            new ForeachAction<String, String>() {
                int recordCounter = 0;
                @Override
                public void apply(final String key, final String value) {
                    if (recordCounter++ % reportInterval == 0) {

                        System.out.println(String.format("Processed %d records so far at %s", recordCounter, Instant.now().toString()));
                        System.out.flush();
                    }
                }
            }
        ).groupByKey()
            .count(Materialized.as("count-store"))
            .toStream()
            .mapValues(countMapper)
            .to("output", Produced.with(stringSerde, stringSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);

        streams.setUncaughtExceptionHandler((t, e) -> {
            System.err.println("FATAL: An unexpected exception " + e);
            e.printStackTrace(System.err);
            System.err.flush();
            shutdown(streams);
        });

        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                System.out.println("Now in RUNNING state " + Instant.now());
                    final Set<ThreadMetadata> threadMetadata = streams.localThreadsMetadata();
                    for (final ThreadMetadata threadMetadatum : threadMetadata) {
                        System.out.println("ACTIVE_TASKS:" + threadMetadatum.activeTasks().size() + " "+ Instant.now());
                    }
            }
        });

        System.out.println("Cleaning up Kafka Streams " + Instant.now());
        streams.cleanUp();
        System.out.println("Start Kafka Streams " + Instant.now());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown(streams);
            System.out.println("Shut down streams now " + Instant.now());
        }));
    }

    private static void shutdown(final KafkaStreams streams) {
        streams.close(Duration.ofSeconds(10));
    }



}
