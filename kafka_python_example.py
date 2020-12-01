from __future__ import absolute_import

from message_bus.bus.bus_frame import Bus
from message_bus.bus import MyCallback
from message_bus import Producer
from message_bus import Consumer
from message_bus.config import Config
from message_bus.bus.topic import Topic
from message_bus.message import Message

from message_bus.utils import log_decorator

def check(topic):
    if 'default' in topic :
        return True
    else:
        return False

@log_decorator
def main():

    #config = Config()
    bus_start = Bus('kafka')
    prod_obj = Producer(bus_start)

    print('*' * 45)
    print('Sending messages . . .')
    msg_obj = Message("This is message", "default","json")
    for i in range(2):
        prod_obj.send(msg_obj)

    cons_obj = Consumer(bus_start)
    print('Subscribing messages . . .')
    subscription = cons_obj.subscribe(['default'], check)
    msg1 = cons_obj.receive(subscription)
    print('*'*45)

    print('Receiving messages 1 . . .')
    for each in msg1:
        print(each)
    print('*' * 45)

    print('Unsubscribing messages . . .')
    unsubscription = cons_obj.unsubscribe(subscription)
    msg2 = cons_obj.receive(unsubscription)

    print('Receiving messages 2 . . .')
    for each in msg2:
        print(each)

if __name__ == "__main__":
    main()



