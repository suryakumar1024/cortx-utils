class BusCallback(object):
    
    def precreate_busclient(self, clnt, role):
        pass

    def postcreate_busclient(self, clnt, role):
        pass

    def precreate_topic(self, topic_name, timeout_ms=None, validate_only=False):
        pass

    def postcreate_topic(self, topic_name, timeout_ms=None, validate_only=False):
        pass

    def pre_send(self, msg, clnt, topic, channel):
        pass

    def post_send(self, msg, clnt, topic, channel):
        pass

    def pre_receive(self, consumer):
        pass

    def post_receive(self, consumer):
        pass

    def pre_subscribe(self, consumer, topic, pattern=None, listener=None):
        pass

    def post_subscribe(self, consumer, topic, pattern=None, listener=None):
        pass




class MyCallback(BusCallback):
    
    def precreate_busclient(self, clnt, role):
        super().precreate_busclient(self, clnt, role)

    def postcreate_busclient(self, clnt, role):
        super().postcreate_busclient(self, clnt, role)
    
    def precreate_topic(self, topic_name, timeout_ms=None, validate_only=False):
        super().precreate_topic(self, topic_name, timeout_ms=None, validate_only=False)
        
    def postcreate_topic(self, topic_name, timeout_ms=None, validate_only=False):
        super().postcreate_topic(self, topic_name, timeout_ms=None, validate_only=False)
    
    def pre_send(self, msg, clnt, topic, channel):
        super().pre_send(self, msg, clnt, topic, channel)

    def post_send(self, msg, clnt, topic, channel):
        super().post_send(self, msg, clnt, topic, channel)
        
    def pre_subscribe(self, consumer, topic, pattern=None, listener=None):
        super().pre_subscribe(self, consumer, topic, pattern=None, listener=None)

    def post_subscribe(self, consumer, topic, pattern=None, listener=None):
        super().post_subscribe(self, consumer, topic, pattern=None, listener=None)

    def pre_receive(self,consumer):
        super().pre_send()

    def post_receive(self, consumer):
        super().post_send()







