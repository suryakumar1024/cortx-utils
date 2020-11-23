import logging
from client import BusClient

log = logging.getLogger(__name__)


class Producer(BusClient):
    def __init__(self, busHandle):
        # self.bus_handle =
        log.info("Bus producer initialized")
        print("Bus producer initialized")
        # self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
        super().__init__(busHandle, 'PRODUCER')

    def send(self, topic, message):
        log.info("Bus send message")
        print("Bus send message")
        # self.producer.send(topic, message)
        # super(BusClient, self).send(topic, message)
        super().send(topic, message)
        log.info("Bus send message complete")
        print("Bus send message complete")