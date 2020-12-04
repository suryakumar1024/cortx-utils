from src.template import Factory
from src.utils.message_bus import KafkaPythonMessageBroker
from src.utils.message_bus.config import KafkaConfig

class KafkaFactory(Factory):

    def __init__(self):
        self.config = KafkaConfig().get_config()
        self.adapter = KafkaPythonMessageBroker(self.config)
        self.admin = self.adapter.create_admin()


