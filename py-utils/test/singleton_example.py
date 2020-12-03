import sys
sys.path.insert(1, '../')

from src.utils.message_bus.bus import MessageBus
from src.utils.message_bus import Producer
from src.utils.message_bus import Consumer
from src.utils.message_bus.message import Message
from src.utils.message_bus.utils import log_decorator


@log_decorator
def main():

    bus_start1 = MessageBus('kafka-python')

    prod_obj = Producer(bus_start1)
    msg_obj = Message("This is message", "testing","json")
    for i in range(2):
        prod_obj.send(msg_obj)
    cons_obj = Consumer(bus_start1)
    subscription = cons_obj.subscribe(['testing'])
    msg1 = cons_obj.receive(subscription)
    for each in msg1:
        print(each)
    unsubscription = cons_obj.unsubscribe(subscription)
    msg2 = cons_obj.receive(unsubscription)
    for each in msg2:
        print(each)

    bus_start2 = MessageBus('confluent-kafka')

    assert id(bus_start1) == id(bus_start2)
    print('Is this Singleton ? ', id(bus_start1) == id(bus_start2))

    prod_obj = Producer(bus_start2)
    msg_obj = Message("This is message", "testing", "json")
    for i in range(2):
        prod_obj.send(msg_obj)

    cons_obj = Consumer(bus_start2)
    subscription = cons_obj.subscribe(['testing'])
    msg1 = cons_obj.receive(subscription)
    for each in msg1:
        print(each)

    unsubscription = cons_obj.unsubscribe(subscription)
    msg2 = cons_obj.receive(unsubscription)
    for each in msg2:
        print(each)

if __name__ == "__main__":
    main()



