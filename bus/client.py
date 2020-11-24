

class BusClient(object):
    def __init__(self, busHandle, role):
        self.role = role
        self.busHandle = busHandle
        self.bus_client = self.busHandle.create(self.role)

    def send(self, message=None):
        self.busHandle.send(self.bus_client, message)

    def receive(self, topic):
        return self.busHandle.receive(self.bus_client, topic)

    def create_producer(self):
        self.busHandle.create(self.role)

    # def create_topic(self):
    #     pass

    def delete_topic(self):
        pass

    def list_topic(self):
        pass
