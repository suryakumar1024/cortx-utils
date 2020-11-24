
class TopicSchema(object):

    def __init__(self):
        self.mapper = {}

    def get_topic(self, message, client):

        message_obj = message.get_message_header()
        # return self.mapper[message_obj.message_type]
        return message_obj.message_type

    def set_topic(self, topic_name, topic):
        # create topic and set it in mapper
        self.mapper[topic_name] = topic

# TopicSchema(['topic1','topic2'])