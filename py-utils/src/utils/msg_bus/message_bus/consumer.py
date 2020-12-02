
from message_bus.bus import BusClient
from message_bus.functor import Callable

class Consumer(BusClient):

    def __init__(self, busHandle):
        super().__init__(busHandle, 'CONSUMER')

    def send(self):
        pass

    def subscribe(self, topic, func_call=None):
        if func_call is not None:
            notifier = Callable(topic, func_call)
        else:
            notifier = None
        return super().subscribe(topic, notifier)

    def unsubscribe(self, subscription):
        return super().unsubscribe(subscription)

    def receive(self, consumer):
        return super().receive(consumer)
