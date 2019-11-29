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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class ConstantProducer {
    private static final Logger LOG = LoggerFactory.getLogger(ConstantProducer.class);

    public static void main(String[] args) {

        if (args.length == 0) {
            LOG.info("Please specify a topic or comma separated list of topics");
            System.exit(1);
        }

        String[] topics = args[0].split(",");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "3");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

            Callback callback = (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Error producing message", exception);
                }
            };

            LOG.info("Sending messages to topics {}", Arrays.toString(topics));


            while (true) {
                final Random random = new Random();
                String key = Integer.toString(random.nextInt(8));
                String value = Instant.now().toString();

                for (String topic : topics) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                    producer.send(record, callback);
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
             Thread.currentThread().interrupt();
        }
    }

}

