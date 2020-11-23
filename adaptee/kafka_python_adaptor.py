
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from adaptee.adaptee import Adaptee

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

    def send(self, producer, topic, msg):
        producer.send(topic, msg)

    def receive(self, consumer, topic):
        consumer.subscribe(topic)
        return consumer

    # def closeChannel(self):
    #     producer.close()
    def create(self, role):
        if role == 'PRODUCER':
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            return producer
        elif role == 'CONSUMER':
            consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest', consumer_timeout_ms=1000)
            return consumer
        else:
            print('pass')
            pass
            #assert(role)

    def create_admin(self):
        pass