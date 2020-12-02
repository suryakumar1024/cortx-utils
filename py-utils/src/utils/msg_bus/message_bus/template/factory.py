from message_bus.adaptee import KafkaAdaptee, ConfluentAdaptee
from message_bus.config import KafkaConfig, ConfluentKafkaConfig

class Factory(object):

    def __init__(self, adaptors):
        self.m_dict = adaptors

    def __call__(self, mq_name):
        return self.m_dict[mq_name]()

class kafkaFactory(Factory):

    def __init__(self):
        return KafkaConfig().get_config(), KafkaAdaptee(self.config), self.adaptor.create_admin()


class ConfluentFactory(Factory):

    def __init__(self):
        return ConfluentKafkaConfig().get_config(), ConfluentAdaptee(self.config), self.adaptor.create_admin()
