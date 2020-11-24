import logging

from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from adaptee.adaptee import Adaptee
from bus.topic_schema import TopicSchema

from utils import log_decorator


class KafkaAdaptee(Adaptee):

    def __init__(self):
        try:
            self.admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
            self.schema = TopicSchema()
        except Exception as e:
            print(e)

    @log_decorator
    def send(self, producer, message):
        topic = self.schema.get_topic(message, producer)
        all_topic_list = self.get_all_topics()
        if topic in all_topic_list:
            producer.send(topic, bytes(message.payload, 'utf-8'))
        else:
            # logging.debug("Topic not exist. Create the topic before sending")
            raise KeyError("Topic not exist. Create the topic before sending")

    def receive(self, consumer, topic):
        consumer.subscribe(topic)
        return consumer

    # def closeChannel(self):
    #     producer.close()
    # @log_decorator
    def create(self, role):
        if role == 'PRODUCER':
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            return producer
        elif role == 'CONSUMER':
            consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)
            return consumer
        else:
            print('pass')
            pass
            #assert(role)

    def create_admin(self):
        pass

    @log_decorator
    def create_topics(self, new_topics, timeout_ms=None, validate_only=False):
        new_topics = NewTopic(name=new_topics, num_partitions=1, replication_factor=1)
        return self.admin.create_topics([new_topics], timeout_ms, validate_only)
        # log.info("Topic Created")

    def get_all_topics(self):
        return self.admin.list_topics()
