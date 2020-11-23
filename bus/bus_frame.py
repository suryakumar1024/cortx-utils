from adaptee.kafka_python_adaptor import KafkaAdaptee
import logging
# logging.basicConfig(filename='log_file.log', encoding='utf-8', level=logging.DEBUG)
log = logging.getLogger(__name__)

class Bus(object):
    def __init__(self, mq_client_name, config):
        log.debug("Init bus class")
        self.config = config

        if mq_client_name == 'kafka':
            self.adaptor = KafkaAdaptee()
        else:
            self.adaptor = KafkaAdaptee()
        self.client_list = []


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
        return self.adaptor.create(self.role)

    def send(self, producer, topic, message):
        # check for topic existence
        log.debug(f"Message '{message}' sending to topic -> {topic} in-progress")
        print(f"Message '{message}' sending to topic -> {topic} in-progress")
        self.adaptor.send(producer, topic, message)
        print(f"Message '{message}' sent to topic -> {topic} in-progress")
        log.debug(f"Message '{message}' sending to topic -> {topic} complete")

    def receive(self, consumer, topic):
        return self.adaptor.receive(consumer, topic)

    # def subscribe(self,topics,pattern=None,listener=None):
    #     log.info("Listening to topic" + " ".join(topics))
    #     print("Listening to topic" + " ".join(topics))
    #     self.bus_consumer.subscribe(topics, pattern, listener)

    def unsubscribe(self):
        pass
    def new_topic(self):
        pass
    def configure(self):
        pass
    def fetch(self):
        pass
    def get_all_topics(self):
        return self.bus_consumer.topics()

# class Config(object):
#     def __init__(self):
#         self.bootstrap_servers = 'localhost:9092'
