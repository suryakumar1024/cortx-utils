
from bus.client import BusClient
from functor import Callable

class Consumer(BusClient):

    def __init__(self, busHandle):
        super().__init__(busHandle, 'CONSUMER')

    def send(self):
        pass

    def subscribe(self, topic, func_call):
    #def subscribe(self, topic):
        notifier = Callable(topic, func_call)
        return super().subscribe(topic, notifier)

    def unsubscribe(self, subscription):
        return super().unsubscribe(subscription)

    def receive(self, consumer):
        return super().receive(consumer)
