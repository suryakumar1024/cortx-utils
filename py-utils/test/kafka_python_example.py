import sys
sys.path.insert(1, '../')

from src.utils.message_bus.bus import MessageBus
from src.utils.message_bus import Producer
from src.utils.message_bus import Consumer
from src.utils.message_bus.message import Message
from src.utils.message_bus.utils import log_decorator



@log_decorator
def main():
    bus_start = MessageBus('kafka-python')
    prod_obj = Producer(bus_start)

    print('*' * 45)
    print('Sending messages . . .')
    msg_obj = Message("This is message", "testing", "json")
    for i in range(2):
        prod_obj.send(msg_obj)

    cons_obj = Consumer(bus_start)
    print('Subscribing messages . . .')
    subscription = cons_obj.subscribe(['testing'])
    msg1 = cons_obj.receive(subscription)
    print('*' * 45)

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



