class BusCallback(object):
    
    def precreate_busclient(self, role):
        pass

    def postcreate_busclient(self, role):
        pass

    def precreate_topic(self, topic_name, timeout_ms=None, validate_only=False):
        pass

    def postcreate_topic(self, topic_name, timeout_ms=None, validate_only=False):
        pass

    def pre_send(self, clnt, topic, msg):
        pass

    def post_send(self, clnt, topic, msg):
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
    
    def precreate_busclient(self, role):
        super().precreate_busclient(role)

    def postcreate_busclient(self, role):
        super().postcreate_busclient(role)
    
    def precreate_topic(self, topic_name, timeout_ms=None, validate_only=False):
        super().precreate_topic(topic_name, timeout_ms=None, validate_only=False)
        
    def postcreate_topic(self, topic_name, timeout_ms=None, validate_only=False):
        super().postcreate_topic(topic_name, timeout_ms=None, validate_only=False)
    
    def pre_send(self, clnt, topic, msg):
        super().pre_send(clnt, topic, msg)

    def post_send(self, clnt, topic, msg):
        super().post_send(clnt, topic, msg)
        
    def pre_subscribe(self, consumer, topic, pattern=None, listener=None):
        super().pre_subscribe(consumer, topic, pattern=None, listener=None)

    def post_subscribe(self, consumer, topic, pattern=None, listener=None):
        super().post_subscribe(consumer, topic, pattern=None, listener=None)

    def pre_receive(self,consumer):
        super().pre_receive(consumer)

    def post_receive(self, consumer):
        super().pre_receive(consumer)







