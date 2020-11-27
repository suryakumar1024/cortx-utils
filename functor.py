class Functor:
    def __init__(self):
        pass

class Callable(Functor):

    def __init__(self, topic, caller):
        self.topic = topic
        self.caller = caller
        
    def get_caller(self):
        return self.caller(self.topic)

