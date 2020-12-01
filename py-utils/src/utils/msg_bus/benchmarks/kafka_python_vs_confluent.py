from __future__ import absolute_import


from bus.bus_frame import Bus
from bus.callback import MyCallback
from producer import Producer
from consumer import Consumer
from config import Config
from bus.topic import Topic
from message import Message

import time

producer_timings = {}
consumer_timings = {}

# def calculate_thoughput(timing, n_messages=1000, msg_size=100):
#     print(f"Processed {n_messages} messsages in {int(timing)} seconds")
#     print(f"{(msg_size * n_messages) / int(timing) / (1024*1024)} MB/s")
#     print(f"{n_messages / int(timing)} Msgs/s")

def calculate_thoughput(timing, n_messages=100, msg_size=100):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing[0]))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing[0] / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing[0]))
    print(f"Total messages received {timing[1]}")


def call_confluent(mq_type, msg_count):
    consumer_start = time.time()
    #config = Config()
    bus_start = Bus(mq_type)
    prod_obj = Producer(bus_start)

    print('*' * 45)
    print(f"Sending messages {mq_type}")
    msg_obj = Message("This is message for bulk test which may contain approximatly 100 characters for testing purpose only to ensure the consistency", "default","json")
    for i in range(msg_count):
        prod_obj.send(msg_obj)
        print(f"Sending messages {mq_type}-->{i}")

    cons_obj = Consumer(bus_start)
    print('*' * 45)
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
    consumer_timing = time.time() - consumer_start
    return [consumer_timing, len(msg1)]

calculate ={}
calculate['kafka'] = call_confluent('kafka', 100)
calculate['confluent'] = call_confluent('confluent', 100)

for each in calculate:
    calculate_thoughput(calculate[each])