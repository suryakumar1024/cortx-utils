from src.utils.message_bus.message import MessageConfig

class MessageHeader(object):
    # def __new__(cls, *args, **kwargs):
    #     pass
    def __init__(self,m_type):
        self.msg_con_obj = MessageConfig()
        self.message_type = self.msg_con_obj.get_message_config(m_type)

    def create(self):
        pass
    def remove(self, type):
        pass
    def update(self, old, new):
        pass