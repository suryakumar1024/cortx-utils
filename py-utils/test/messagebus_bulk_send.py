import sys
sys.path.insert(1, '../')

from src.utils.message_bus.bus.bus_frame import MessageBus
from src.utils.message_bus import Producer
from src.utils.message_bus import Consumer
from src.utils.message_bus.message import Message
from src.utils.message_bus.utils import log_decorator

def check(topic):
    if 'default' in topic :
        return True
    else:
        return False

# @log_decorator.log_decorator()
def main():

    #config = Config()
    bus_start = MessageBus('confluent-kafka')
    prod_obj = Producer(bus_start)

    print('*' * 45)
    print('Sending messages . . .')
    msg_obj_list = []
    for i in range(20):
        msg_obj_list.append(Message(f"{str(i)*5}", "default","json"))
    prod_obj.bulk_send("default", msg_obj_list)

    cons_obj = Consumer(bus_start)
    print('Subscribing messages . . .')
    subscription = cons_obj.subscribe(['default'])
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



