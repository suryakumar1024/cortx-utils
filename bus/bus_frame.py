from adaptee.kafka_python_adaptor import KafkaAdaptee
from config import KafkaConfig
import logging
log = logging.getLogger(__name__)

from bus.topic_schema import TopicSchema
from bus.topic import Topic
from utils import log_decorator


class Bus(object):

    @log_decorator
    def __init__(self, mq_bus_name, bus_callback = None):
        # log.debug("Init bus class")
        self.config = None
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
            # self.config_obj = KafkaConfig()
            self.config = KafkaConfig().get_config()
            self.adaptor = KafkaAdaptee(self.config)
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

    def receive(self, consumer):
        if self.bus_callback is not None:
            self.bus_callback.pre_receive(consumer)
        consumer_obj = self.adaptor.receive(consumer)
        if self.bus_callback is not None:
            self.bus_callback.post_receive(consumer)
        return consumer_obj

    def subscribe(self, consumer, topic, pattern=None, listener=None):
        if self.bus_callback is not None:
            self.bus_callback.pre_subscribe(self, consumer, topic, pattern=None, listener=None)
        log.info("Listening to topic " + " ".join(topic))
        print("Listening to topic " + " ".join(topic))
        subscribe_obj =  self.adaptor.subscribe(consumer, topic)
        if self.bus_callback is not None:
            self.bus_callback.post_subscribe(self, consumer, topic, pattern=None, listener=None)
        return subscribe_obj

    def unsubscribe(self, subscription):
        return self.adaptor.unsubscribe(subscription)

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
        print('inside bus get topic')
        return self.adaptor.get_all_topics()

