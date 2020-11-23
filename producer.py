from client import busClient

class Producer(busClient):

    def __init__(self, busHandle):
        super().__init__(busHandle, 'PRODUCER')


    def send(self, topic, message):
        super().send(topic, message)