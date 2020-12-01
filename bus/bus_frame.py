# Kafka-Python
from adaptee.kafka_python_adaptor import KafkaAdaptee
# Confluent-kafka-python
from adaptee.confluent_kafka_adaptor import ConfluentAdaptee
from config import KafkaConfig, ConfluentKafkaConfig
import logging
log = logging.getLogger(__name__)
from template.singleton import Singleton
from bus.topic_schema import TopicSchema
from bus.topic import Topic
from utils import log_decorator


class Bus(metaclass=Singleton):

    @log_decorator
    def __init__(self, mq_bus_name, bus_callback = None):
        # log.debug("Init bus class")
        self.config = None
        self.notifier = None #notifier callable
        self.mapper = {}
        self.callables = {}
        self.mq_bus_name = mq_bus_name
        self.bus_callback = bus_callback
        # Do configurations here
        self.__load_adaptor(mq_bus_name)
        if self.config is not None:
            self.__load_topic()

        self.schema = TopicSchema()
        self.client_list = []

    def __load_adaptor(self, mq_bus_name):
        if mq_bus_name == 'kafka':
            self.config = KafkaConfig().get_config()
            self.adaptor = KafkaAdaptee(self.config)
            self.admin = self.adaptor.create_admin()
        elif mq_bus_name == 'confluent':
            self.config = ConfluentKafkaConfig().get_config()
            self.adaptor = ConfluentAdaptee(self.config)
            self.admin = self.adaptor.create_admin()
        else:
            self.adaptor = KafkaAdaptee(self.config)

    def __load_topic(self):
        # {bus:"kafka", topics:[{name:"Alert", replication_factor:3, policy:"Remove_on_ACK"}], }
        # will get the schema from config file . convert the string in config.schema
        # to TopicInMessage using factory patter
        self.schema = TopicSchema()
        for t in self.config['topics']:
            topic = Topic(t)
            print('*'*10, topic.name)
            self.schema.set_topic(topic.name, topic)

    def register_client(self, cls):
        self.client_list.push(cls)
        pass

    def set_producer(self, Producer):
        # check MQ and it's configuration here
        return producer_cls(bootstrap_servers='localhost:9092')

    def set_consumer(self, Consumer):
        # check MQ and it's configuration here
        return consumer_cls(bootstrap_servers='localhost:9092', auto_offset_reset='earliest',
                            consumer_timeout_ms=1000)

    def set_admin(self, Admin):
        # check MQ and it's configuration here
        return client_cls()

    def create(self, role):
        self.role = role
        if self.bus_callback is not None:
            self.precreate_busclient(self, role)
        create_busclient = self.adaptor.create(self.role)
        if self.bus_callback is not None:
            self.postcreate_busclient(self, role)
        return create_busclient

    @log_decorator
    def send(self, producer, message):
        topic = self.schema.get_topic(message, producer)
        if self.bus_callback is not None:
            self.bus_callback.pre_send(producer, topic, message)
        print(f"Message '{message.payload}' sending to topic -> in-progress")
        all_topic_list = self.get_all_topics()
        if topic in all_topic_list:
        #if True:
            self.adaptor.send(producer, topic, bytes(message.payload, 'utf-8'))
        else:
            # logging.debug("Topic not exist. Create the topic before sending")
            raise KeyError("Topic not exist. Create the topic before sending")
        # Will post send consider exception too
        if self.bus_callback is not None:
            self.bus_callback.post_send(producer, topic, message)


    def get_topic(self, client, message):
        return self.schema.get_topic(client, message)

    def receive(self, consumer ):
        if self.bus_callback is not None:
            self.bus_callback.pre_receive(consumer)
        consumer_obj = self.adaptor.receive(consumer)
        # Need to fix the code -Surya
        # functor_consumer_obj = [i for i in consumer_obj if self.notifier.get_caller()]
        functor_consumer_obj = consumer_obj
        if self.bus_callback is not None:
            self.bus_callback.post_receive(consumer)
        return functor_consumer_obj

    def subscribe(self, consumer, topic, notifier, pattern=None, listener=None):
        # This doesn't receive any consumer message itself. Need to use receive to receive message packets
        self.notifier = notifier
        if self.bus_callback is not None:
            self.bus_callback.pre_subscribe(self, consumer, topic, pattern=None, listener=None)
        print("Listening to topic " + " ".join(topic))
        self.mapper[consumer] = topic
        self.callables[consumer] = {}
        self.callables[consumer][notifier] = topic
        subscribe_obj =  self.adaptor.subscribe(consumer, topic)
        if self.bus_callback is not None:
            self.bus_callback.post_subscribe(self, consumer, topic, pattern=None, listener=None)
        return subscribe_obj

    def unsubscribe(self, consumer):
        if consumer:
            del self.mapper[consumer]
            del self.callables[consumer]
        return self.adaptor.unsubscribe(consumer)

    def create_topic(self,topic_name, timeout_ms=None, validate_only=False):
        if self.bus_callback is not None:
            self.bus_callback.precreate_topic(self,topic_name, timeout_ms=None, validate_only=False)
        self.adaptor.create_topics(topic_name, timeout_ms, validate_only)
        if self.bus_callback is not None:
            self.bus_callback.postcreate_topic(self,topic_name, timeout_ms=None, validate_only=False)

    def configure(self):
        pass

    def fetch(self):
        pass

    def get_all_topics(self):
        # Will return all created topics
        return self.adaptor.get_all_topics()

