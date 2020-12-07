from src.template import Factory
from src.utils.message_bus.confluent_kafka_message_broker import ConfluentKafkaMessageBroker
from src.utils.message_bus.config import Config

class ConfluentFactory(Factory):

    def __init__(self):
        config = Config()
        self.config = config.get_config()
        self.adapter = ConfluentKafkaMessageBroker(self.config)
        self.admin = self.adapter.create_admin()