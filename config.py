
class Config(object):
    def __init__(self):
        pass


class KafkaConfig(Config):
    def __init__(self):
        pass
    def get_config(self):
        #Read from file
        self.config = {
            'bus': "kafka",
            'topics': [{'name': "Alert", 'replication_factor': 3, 'policy': "Remove_on_ACK"}],
            'producer': [{'bootstrap_servers': 'localhost:9092'}],
            'consumer': [{'bootstrap_servers': 'localhost:9092', 'auto_offset_reset': 'earliest', 'consumer_timeout_ms': 1000}]
        }
        return self.config