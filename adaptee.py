from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic

class Adaptee(object):
    def __init__(self):
        pass

    def send(self):
        pass

    def receive(self):
        pass

    def create_producer(self):
        pass

    def create_consumer(self):
        pass

    def create_admin(self):
        pass

class KafkaAdaptee(Adaptee):

    def __init__(self):
        try:
            self.admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        except Exception as e:
            print(e)

    def create_topic(self, topic):
        new_topic = NewTopic(name=topic,
                         num_partitions=1,
                         replication_factor=1)
        self.admin.create_topics([new_topic])

    def send(self, topic, msg):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.send(topic, msg)
        producer.close()

    def receive(self, topic):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(topic)
        for msg in consumer:
            print(msg)

    def create_producer(self):
        pass

    def create_consumer(self):
        pass

    def create_admin(self):
        pass

