from message_bus.adaptee import KafkaAdaptee, ConfluentAdaptee
from message_bus.config import KafkaConfig, ConfluentKafkaConfig

class Factory(object):

    def __init__(self, adaptors):
        self.m_dict = adaptors

    def __call__(self, mq_name):
        return self.m_dict[mq_name]()

class kafkaFactory(Factory):

    def __init__(self):
        self.config = KafkaConfig().get_config()
        self.adaptor = KafkaAdaptee(self.config)
        self.admin = self.adaptor.create_admin()


class ConfluentFactory(Factory):

    def __init__(self):
        self.config = ConfluentKafkaConfig().get_config()
        self.adaptor = ConfluentAdaptee(self.config)
        self.admin = self.adaptor.create_admin()




if __name__ == "__main__":
    f = Factory("kafka-python")