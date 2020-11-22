
import main as m
from main import Bus

class busClient(object):
    def __init__(self, mq):
        self.busHandle = Bus(mq)

    def send(self, topic, message):
        self.busHandle.send(topic, message)

    def receive(self, topic):
        self.busHandle.receive(topic)

    def create_topic(self):
        pass

    def delete_topic(self):
        pass

    def list_topic(self):
        pass

class Admin(busClient):

    def __init__(self):
        pass

    def create_topic(self):
        pass

    def delete_topic(self):
        pass

    def list_topic(self):
        pass


class Producer(busClient):

    def __init__(self, busHandle):
        self.busHandle = busHandle

    def send(self, topic, message):
        super().send(topic, message)

class Consumer(busClient):

    def __init__(self, busHandle):
        self.busHandle = busHandle

    def send(self):
        pass

    def receive(self, topic):
        super().receive(topic)

