from messageBus.adaptee import KafkaAdaptee
import logging

log = logging.getLogger(__name__)


class Bus(object):
    def __init__(self, mq_client_name):
        log.debug("Init Bus class")
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

    def send(self, topic, message):
        # check for topic existence
        log.debug(f"Message '{message}' sending to topic -> {topic} in-progress")
        print(f"Message '{message}' sending to topic -> {topic} in-progress")
        self.adaptor.send(topic, message)
        print(f"Message '{message}' sent to topic -> {topic} in-progress")
        log.debug(f"Message '{message}' sending to topic -> {topic} complete")

    def receive(self, topic):
        self.adaptor.receive(topic)

    def subscribe(self,topics,pattern=None,listener=None):
        log.info("Listening to topic" + " ".join(topics))
        print("Listening to topic" + " ".join(topics))
        self.bus_consumer.subscribe(topics, pattern, listener)

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
