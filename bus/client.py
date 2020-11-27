

class BusClient(object):
    def __init__(self, busHandle, role):
        self.role = role
        self.busHandle = busHandle
        self.bus_client = self.busHandle.create(self.role)

    def send(self, message=None):
        self.busHandle.send(self.bus_client, message)

    def receive(self, consumer):
        return self.busHandle.receive(consumer)

    def create(self):
        self.busHandle.create(self.role)

    def subscribe(self, topic, notifier):
        # self.mapper[]
        return self.busHandle.subscribe(self.bus_client, topic, notifier)
    
    def unsubscribe(self, subscription):
        return self.busHandle.unsubscribe(subscription)
