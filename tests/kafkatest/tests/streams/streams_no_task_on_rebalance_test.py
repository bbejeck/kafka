# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from kafkatest.services.streams import StreamsTasksNotFoundService
from ducktape.tests.test import Test
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.verifiable_producer import VerifiableProducer
import time


class StreamsTasksAvailableAfterRebalance(Test):
    """
    This test validates using standby tasks helps with rebalance times
    additionally verifies standby replicas continue to work in the
    face of continual changes to streams code base
    """

    streams_source_topic = "input"
    streams_sink_topic = "output"
    client_id = "stream-broker-resilience-verify-consumer"
    processing_message = "Processed [0-9]* records so far"
    shut_down_message = "Shut down streams now"
    running_state_message = "Now in RUNNING state"
    sleep_time_secs = 120
    test_iterations = 25

    def __init__(self, test_context):
        super(StreamsTasksAvailableAfterRebalance, self).__init__(test_context)
        self.topics = {
            self.streams_source_topic: {'partitions': 6,
                                        'replication-factor': 1},
            self.streams_sink_topic: {'partitions': 1,
                                      'replication-factor': 1}
        }
        self.zookeeper = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=3,
                                  zk=self.zookeeper, topics=self.topics)

        self.producer = VerifiableProducer(self.test_context,
                                           1,
                                           self.kafka,
                                           self.streams_source_topic,
                                           repeating_keys=6,
                                           acks=1)

    def test_tasks_always_found_on_rebalance(self):
        self.zookeeper.start()
        self.kafka.start()

        self.producer.start()

        processor_1 = StreamsTasksNotFoundService(self.test_context, self.kafka)
        processor_2 = StreamsTasksNotFoundService(self.test_context, self.kafka)

        processor_1.start()
        processor_2.start()

        self.verify_state(processor_1, self.processing_message)
        self.verify_state(processor_2, self.processing_message)

        # Let test run to build up enough state
        self.logger.info("Pausing test for 2 minutes to build up state")
        time.sleep(self.sleep_time_secs)
        self.logger.info("Resuming test")

        for x in range(self.test_iterations):
            num = x + 1
            self.logger.info("Starting iteration %s", num)
            self.stop_and_verify(processor_1)
            self.verify_state(processor_2, "ACTIVE_TASKS:6")

            processor_1.start()
            self.verify_state(processor_1, self.processing_message)

            self.stop_and_verify(processor_2)
            self.verify_state(processor_1, "ACTIVE_TASKS:6")

            processor_2.start()
            self.verify_state(processor_2, self.processing_message)

        self.producer.stop()
        self.kafka.stop()
        self.zookeeper.stop()

    def verify_state(self, processor, pattern):
        self.logger.info("Verifying %s processing pattern in STDOUT_FILE" % pattern)
        with processor.node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            monitor.wait_until(pattern,
                               timeout_sec=120,
                               err_msg="Never saw processing of %s " % pattern + str(processor.node.account))

    def stop_and_verify(self, processor):
        node = processor.node
        with node.account.monitor_log(processor.STDOUT_FILE) as monitor:
            processor.stop()
            monitor.wait_until(self.shut_down_message,
                               timeout_sec=60,
                               err_msg="Never saw processing of %s " % self.shut_down_message + str(processor.node.account))
