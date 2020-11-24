
class Config(object):
    def __init__(self):
        self.bootstrap_servers = 'localhost:9092'
        self.auto_offset_reset = 'earliest'
        self.consumer_timeout_ms = 1000

#{bus:"kafka", topics:[{name:"Alert", replication_factor:3, policy:"Remove_on_ACK"}], producer:[]}

class KafkaConfig(Config):
    def __init__(self):
        self.bootstrap_servers = 'localhost:9092'
        self.auto_offset_reset = 'earliest'
        self.consumer_timeout_ms = 1000

