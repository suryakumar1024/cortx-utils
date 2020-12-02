
class Config(object):
    def __init__(self):
        pass


class KafkaConfig(Config):
    def __init__(self):
        #Read from file
        self.config = {
            'bus': "kafka",
            'topics': [{'name': "Alert", 'replication_factor': 3, 'policy': "Remove_on_ACK"}],
            'client': [{'bootstrap_servers': 'localhost:9092'}],
            'producer': [{'bootstrap_servers': 'localhost:9092'}],
            'consumer': [{'bootstrap_servers': 'localhost:9092', 'auto_offset_reset': 'earliest', 'consumer_timeout_ms': 1000}]
        }

    def get_config(self):
        return self.config

class ConfluentKafkaConfig(Config):
    def __init__(self):
        self.config = {
            'bus': "confluent",
            'topics': [{'name': "Alert", 'replication_factor': 3, 'policy': "Remove_on_ACK"}],
            'client': [{'bootstrap.servers': 'localhost:9092'}],
            'producer': [{'bootstrap.servers': 'localhost:9092'}],
            'consumer': [{'bootstrap.servers': 'localhost:9092','session.timeout.ms': 6000,
            'group.id':'default'}]
        }
    def get_config(self):
        #Read from file
        return self.config