#!/usr/bin/env python3

# CORTX-Py-Utils: CORTX Python common library.
# Copyright (c) 2020 Seagate Technology LLC and/or its Affiliates
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.


from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from src.utils.message_bus import MessageBroker
from src.utils.message_bus.exceptions import ClientNotfoundError

class KafkaPythonMessageBroker(MessageBroker):

    def __init__(self, config):
        try:
            self.config = config
        except Exception as e:
            print(e)

    def create_admin(self):
        config = self.config['client'][0]
        try:
            self.admin = KafkaAdminClient(**config)
        except:
            raise ClientNotfoundError
        return self.admin

    def create_topic(self, topic):
        new_topic = NewTopic(name=topic,
                         num_partitions=1,
                         replication_factor=1)
        self.admin.create_topics([new_topic])

    def send(self, producer, topic, message):
        producer.send(topic, message)

    def receive(self, consumer):
        if consumer:
            return list(consumer)
        else:
            return 'No Subscription'

    def subscribe(self, consumer, topic=None, listener='listen'):
        consumer.subscribe(topic)
        return consumer
        
    def unsubscribe(self,consumer):
        consumer.unsubscribe()
        return consumer

    def create(self, role):
        if role == 'PRODUCER':
            config = self.config['producer'][0]
            producer = KafkaProducer(**config)
            return producer
        elif role == 'CONSUMER':
            config = self.config['consumer'][0]
            consumer = KafkaConsumer(**config)
            return consumer
        else:
            assert role == 'PRODUCER' or role == 'CONSUMER'

    def create_topics(self, new_topics, timeout_ms=None, validate_only=False):
        new_topics = NewTopic(name=new_topics, num_partitions=1, replication_factor=1)
        return self.admin.create_topics([new_topics], timeout_ms, validate_only)

    def get_all_topics(self):
        return self.admin.list_topics()