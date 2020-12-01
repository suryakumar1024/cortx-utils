from bus.bus_frame import Bus

from bus.callback import MyCallback
from producer import Producer
from consumer import Consumer
from config import Config
from bus.topic import Topic
from message import Message

from utils import log_decorator

def check(topic):
    if 'default' in topic :
        return True
    else:
        return False

@log_decorator
def main():

    #config = Config()
    bus_start = Bus('confluent')
    prod_obj = Producer(bus_start)

    print('*' * 45)
    print('Sending messages . . .')
    msg_obj = Message("This is message", "default","json")
    for i in range(20):
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

if __name__ == "__main__":
    main()


