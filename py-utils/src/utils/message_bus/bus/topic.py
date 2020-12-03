from kafka.admin import KafkaAdminClient

from src.utils.message_bus.utils import log_decorator

class Topic(object):

    def __init__(self, topic_config):
        self.name = topic_config['name']
        self.replication_factor = topic_config['replication_factor']
        self.policy = topic_config['policy'] # Values to be set in bus.__init__ and It's class type.

    @log_decorator
    def create(self, topic_name, timeout_ms=None, validate_only=False):
        return self.bus_handle.create_topic(topic_name, timeout_ms, validate_only)

    def get_topic(self,message):
        return self.bus_handle.get_topic(message)

    def get_all_topics(self):
        return self.bus_handle.get_all_topics()
    # CURD
    # replication
    # Update policy