from bus.bus_frame import Bus
from producer import Producer
from consumer import Consumer
from bus.topic import Topic
from message import Message

from utils import log_decorator

@log_decorator
def main():
    bus_obj = Bus('kafka', 'cfg')
    # Producer
    prod_obj = Producer(bus_obj)
    # topic_obj = Topic(bus_obj)
    # topic_obj.create('new-topic')
    # print(topic_obj.get_all_topics())
    # msg_obj = Message({host:"127.127.0.0", module:"sspl", msg: 'this is message body', type:alert}, "Default",
    # "json")
    msg_obj = Message("This is message", "Default","json")
    #
    for i in range(10):
        prod_obj.send(msg_obj)

    # Consumer
    cons_obj = Consumer(bus_obj)
    cons_msg = cons_obj.receive(['Default'])
    for each in cons_msg:
        print(each)


if __name__ == "__main__":
    main()



