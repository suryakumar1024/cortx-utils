from kafka.admin import KafkaAdminClient

from utils import log_decorator

class Topic(object):

    def __init__(self, bus_handle):
        self.bus_handle = bus_handle
        self.replication_factor = 3
        self.topic_policy = 'TOPIC_POLICY' # Values to be set in bus.__init__ and It's class type.

    @log_decorator
    def create(self, topic_name, timeout_ms=None, validate_only=False):
        return self.bus_handle.create_topic(topic_name, timeout_ms, validate_only)