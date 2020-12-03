
from src.utils.message_bus.utils import validate_string
from src.utils.message_bus.constants import MessageConst as msg_c

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


# {bus:"kafka", topics:[{name:"Alert", replication_factor:3, policy:"Remove_on_ACK"}], }