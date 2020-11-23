
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from adaptee.adaptee import Adaptee

from utils import log_decorator


class KafkaAdaptee(Adaptee):

    def __init__(self):
        try:
            self.admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        except Exception as e:
            print(e)

    def send(self, producer, topic, msg):
        producer.send(topic, msg)

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
