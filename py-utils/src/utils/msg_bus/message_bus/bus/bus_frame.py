import logging
log = logging.getLogger(__name__)

# Kafka-Python
from message_bus.adaptee import KafkaAdaptee
from message_bus.bus.callback import MyCallback
from message_bus.template import kafkaFactory, ConfluentFactory
from message_bus.template import Factory, Singleton
from message_bus.bus.topic_schema import TopicSchema
from message_bus.bus.topic import Topic
from message_bus.utils import log_decorator


class Bus(metaclass=Singleton):

    @log_decorator
    def __init__(self, mq_bus_name, bus_callback=MyCallback()):
        # log.debug("Init bus class")
        self.config = None
        self.notifier = None #notifier callable
        self.mapper = {}
        self.callables = {}
        self.mq_bus_name = mq_bus_name
        self.bus_callback = bus_callback
        self.m_factory = Factory({
            "kafka-python": kafkaFactory,
            "confluent-kafka": ConfluentFactory
        })
        self.__load_adaptor(mq_bus_name)
        if self.config is not None:
            self.__load_topic()

        self.schema = TopicSchema()
        self.client_list = []

    def __load_adaptor(self, mq_bus_name):
        #config
        self.config, self.adaptor, self.admin = self.m_factory("kafka-python")


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

        self.bus_callback.precreate_busclient(self.role)
        create_busclient = self.adaptor.create(self.role)
        self.bus_callback.postcreate_busclient(self.role)

        return create_busclient

    @log_decorator
    def send(self, producer, message):
        topic = self.schema.get_topic(message, producer)
        self.bus_callback.pre_send(producer, topic, message)

        all_topic_list = self.get_all_topics()
        if topic in all_topic_list:
        #if True:
            self.adaptor.send(producer, topic, bytes(message.payload, 'utf-8'))
        else:
            # logging.debug("Topic not exist. Create the topic before sending")
            raise KeyError("Topic not exist. Create the topic before sending")
        # Will post send consider exception too
        self.bus_callback.post_send(producer, topic, message)


    def get_topic(self, client, message):
        return self.schema.get_topic(client, message)

    def receive(self, consumer ):
        self.bus_callback.pre_receive(consumer)
        consumer_obj = self.adaptor.receive(consumer)

        if self.notifier is not None:
            consumer_obj = self.notifier.get_caller(consumer_obj)

        self.bus_callback.post_receive(consumer)
        return consumer_obj

    def subscribe(self, consumer, topic, notifier, pattern=None, listener=None):
        # This doesn't receive any consumer message itself. Need to use receive to receive message packets
        if notifier is not None:
            self.notifier = notifier

        self.bus_callback.pre_subscribe(consumer, topic, pattern=None, listener=None)

        self.mapper[consumer] = topic
        self.callables[consumer] = {}
        self.callables[consumer][notifier] = topic
        subscribe_obj =  self.adaptor.subscribe(consumer, topic)

        self.bus_callback.post_subscribe(consumer, topic, pattern=None, listener=None)
        return subscribe_obj

    def unsubscribe(self, consumer):
        if consumer:
            del self.mapper[consumer]
            del self.callables[consumer]
        return self.adaptor.unsubscribe(consumer)

    def create_topic(self,topic_name, timeout_ms=None, validate_only=False):
        self.bus_callback.precreate_topic(topic_name, timeout_ms=None, validate_only=False)
        self.adaptor.create_topics(topic_name, timeout_ms, validate_only)
        self.bus_callback.postcreate_topic(topic_name, timeout_ms=None, validate_only=False)

    def configure(self):
        pass

    def fetch(self):
        pass

    def get_all_topics(self):
        # Will return all created topics
        return self.adaptor.get_all_topics()

