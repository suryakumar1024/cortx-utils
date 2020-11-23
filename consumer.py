from bus.client import BusClient

class Consumer(BusClient):

    def __init__(self, busHandle):
        super().__init__(busHandle, 'CONSUMER')

    def send(self):
        pass

    def receive(self, topic):
        return super().receive(topic)