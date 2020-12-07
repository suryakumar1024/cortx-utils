import logging
from src.utils.message_bus.bus import BusClient

log = logging.getLogger(__name__)


class Producer(BusClient):
    def __init__(self, busHandle):
        # self.bus_handle =
        log.info("Bus producer initialized")
        print("Bus producer initialized")
        # self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
        super().__init__(busHandle, 'PRODUCER')

    def bulk_send(self, topic, list_of_messages):
        # log.info("Bus bulk send messages")
        super().bulk_send(topic, list_of_messages)
        # log.info("Bus bulk send message completed")

    def send(self, message):
        log.info("Bus send message")
        print("Bus send message")
        super().send(message)
        log.info("Bus send message complete")
        print("Bus send message complete")