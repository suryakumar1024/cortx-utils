
from bus.client import BusClient

class Consumer(BusClient):

    def __init__(self, busHandle):
        super().__init__(busHandle, 'CONSUMER')

    def send(self):
        pass

    def subscribe(self, topic):
        return super().subscribe(topic)

    def unsubscribe(self, subscription):
        return super().unsubscribe(subscription)

    def receive(self, consumer):
        return super().receive(consumer)
