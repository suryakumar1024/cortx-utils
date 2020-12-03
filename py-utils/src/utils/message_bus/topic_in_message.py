from src.utils.message_bus.bus.topic_schema import TopicSchema


class TopicInMessage(TopicSchema):

    def get_topic(self, message, client, bus_handler):
        return message.get_type()