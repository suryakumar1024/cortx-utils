import logging
import sys
from collections import namedtuple

from confluent_kafka.cimpl import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic, ClusterMetadata
from message_bus.adaptee import Adaptee
from message_bus.utils import log_decorator

ConsumerRecord = namedtuple("ConsumerRecord",
                            ["topic", "message", "partition", "offset", "key"])


class ConfluentAdaptee(Adaptee):

    def __init__(self, config):
        try:
            self.config = config
            self.mapper = {}
        except Exception as e:
            print(e)

    def create_admin(self):
        config = self.config['client'][0]
        # self.admin = AdminClient(**config)
        self.admin = AdminClient(config)
        return self.admin

    def create_topic(self, topic):
        new_topic = NewTopic(name=topic,
                             num_partitions=1,
                             replication_factor=1)
        self.admin.create_topics([new_topic])

    @log_decorator
    def send(self, producer, topic, message):
        producer.produce(topic, message)

    def receive(self, consumer):
        # if consumer:
        #     return list(consumer)
        # else:
        #     return 'No Subscription'

        # finally:
        # Close down consumer to commit final offsets.
        # consumer.close()
        msg_list = self.receive_subscribed_topic(consumer)
        return msg_list

    def receive_subscribed_topic(self, consumer):
        try:
            while True:
                msg = consumer.poll(timeout=0.5)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    # Proper message
                    sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                     (msg.topic(), msg.partition(), msg.offset(),
                                      str(msg.key())))
                    yield ConsumerRecord(msg.topic(), msg.value(), msg.partition(), msg.offset(), str(msg.key()))

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

    def subscribe(self, consumer, topic=None, listener='listen'):
        self.mapper[consumer] = topic
        consumer.subscribe(topic)
        return consumer

    def unsubscribe(self, consumer):
        print('mapper', self.mapper)
        if consumer:
            del self.mapper[consumer]
        print('mapper', self.mapper)
        consumer.unsubscribe()
        # consumer.close()
        return consumer

    # def closeChannel(self):
    #     producer.close()
    # @log_decorator
    def create(self, role):
        if role == 'PRODUCER':
            config = self.config['producer'][0]
            producer = Producer(**config)
            return producer
        elif role == 'CONSUMER':
            config = self.config['consumer'][0]
            consumer = Consumer(**config)
            return consumer
        else:
            print('pass')
            pass
            # assert(role)

    @log_decorator
    def create_topics(self, new_topics, timeout_ms=None, validate_only=False):
        new_topics = NewTopic(name=new_topics, num_partitions=1, replication_factor=1)
        return self.admin.create_topics([new_topics], timeout_ms, validate_only)
        # log.info("Topic Created")

    def get_all_topics(self):
        all_topics = self.admin.list_topics()
        all_topics = [topic for topic in all_topics.topics]
        return all_topics
