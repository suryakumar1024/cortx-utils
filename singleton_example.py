from bus.bus_frame import Bus
from bus.callback import MyCallback
from producer import Producer
from consumer import Consumer
from config import Config
from bus.topic import Topic
from message import Message

from utils import log_decorator

@log_decorator
def main():


    bus_start1 = Bus('kafka')
    bus_start2 = Bus('kafka')
    
    assert id(bus_start1) == id(bus_start2)

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


