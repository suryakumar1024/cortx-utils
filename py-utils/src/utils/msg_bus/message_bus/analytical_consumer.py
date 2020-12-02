from message_bus import Consumer
from message_bus.utils import add, check_topic, last_one_hour_data

class AnalyticalConsumer(Consumer):
    def __init__(self):
        self.method = None

    def query(self, regex):
        pass

    def get_stats(self):
        pass

    def receive_aggregate(self, operation):
        fragments = {
            'add': add,
            'check_topic': check_topic,
            'last_one_hour_data': last_one_hour_data
        }
        #self.method = [v for k, v in fragments.items() if k == callable_func]
        # for k, v in fragments.items():
        #     if k == operation:
        self.method = fragments[operation]
        return self.method
