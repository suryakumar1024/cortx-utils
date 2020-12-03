from src.utils.message_bus.constants import MessageConst as msg_c
from src.utils.message_bus.utils import validate_string


class MessageHeader(object):

    def __init__(self,m_type):
        self.msg_con_obj = MessageConfig()
        self.message_type = self.msg_con_obj.get_message_config(m_type)

    def create(self):
        pass
    def remove(self, type):
        pass
    def update(self, old, new):
        pass

class Message(object):

    def __init__(self, msg, m_type=None, m_format = None):
        self.payload = msg if m_format is None else self.format_message(msg, m_format)
        self.m_header = MessageHeader(m_type)
        self.m_formator = m_format

    def get_message_header(self): # make it protected
        # if type is not there.
        return self.m_header if self.m_header is not None else MessageHeader(msg_c.DEFAULT_MESSAGE_TYPE)

    def get_message_type(self):
        if self.m_header is not None:
            message_type = self.m_header.message_type
        else:
            raise KeyError("Message type not exists")
        return message_type

    def get_message_attributes(self, attribute):
        pass

    def get_message(self):
        return self.payload if self.payload is not None else None

    def format_message(self, msg, m_format):
        return msg


class MessageConfig(object):

    def __init__(self):
        self.message_type = msg_c.MESSAGE_TYPE_LIST

    @validate_string
    def create(self, m_type):
        pass

    def get_message_config(self, m_type):
        if m_type in self.message_type:
            return m_type
        else:
            raise KeyError("Message configuration not found")

class MessageFormat(object):
    pass

