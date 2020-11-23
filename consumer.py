from client import busClient

class Consumer(busClient):

    def __init__(self, busHandle):
        super().__init__(busHandle, 'CONSUMER')

    def send(self):
        pass

    def receive(self, topic):
        super().receive(topic)