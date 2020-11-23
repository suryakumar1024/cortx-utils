
class Message(object):

    def __init__(self, msg, m_type=None, m_format = None):
        self.payload = msg
        self.m_type = m_type
        self.m_formator = m_format