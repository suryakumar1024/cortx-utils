import logging

from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from adaptee.adaptee import Adaptee

from utils import log_decorator


class KafkaAdaptee(Adaptee):

    def __init__(self, config):
        try:
            self.config = config
            print(self.config)
            self.admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
            self.mapper = {}
        except Exception as e:
            print(e)

    def create_topic(self, topic):
        new_topic = NewTopic(name=topic,
                         num_partitions=1,
                         replication_factor=1)
        self.admin.create_topics([new_topic])

    @log_decorator
    def send(self, producer, topic, message):
        producer.send(topic, message)

    def receive(self, consumer):
        if consumer:
            return list(consumer)
        else:
            return 'No Subscription'

    def subscribe(self, consumer, topic=None, listener='listen'):
        self.mapper[consumer][topic] = listener
        consumer.subscribe(topic)
        #add listner
        return consumer
        
    def unsubscribe(self,consumer):
        self.mapper = {}
        consumer.unsubscribe()
        return consumer

    # def closeChannel(self):
    #     producer.close()
    # @log_decorator
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
