from src.utils.message_bus.constants import MessageConst as msg_c
from src.utils.message_bus.message import MessageHeader

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