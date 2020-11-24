class BusCallback(object):

    def pre_send(self, msg, clnt, topic, channel):
        pass

    def post_send(self, msg, clnt, topic, channel):
        pass

    def pre_receive(self, msg, clnt, topic, channel):
        super.pre_send()

    def post_receive(self, msg, clnt, topic, channel):
        super.post_send()

    def precreate_busclient(self, clnt, role):
        pass

    def postcreate_busclient(self, clnt, role):
        pass


class MyCallback(BusCallback):
    def pre_send(self, msg, clnt, topic, channel):
        super.pre_send()

    def post_send(self, msg, clnt, topic, channel):
        super.post_send()

    def pre_receive(self, msg, clnt, topic, channel):
        super.pre_send()

    def post_receive(self, msg, clnt, topic, channel):
        super.post_send()





