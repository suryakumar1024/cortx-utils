from src.template import Factory
from src.utils.message_bus import ConfluentKafkaMessageBroker
from src.utils.message_bus.config import ConfluentKafkaConfig

class ConfluentFactory(Factory):

    def __init__(self):
        self.config = ConfluentKafkaConfig().get_config()
        self.adapter = ConfluentKafkaMessageBroker(self.config)
        self.admin = self.adapter.create_admin()