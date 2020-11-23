from bus.bus_frame import Bus
from producer import Producer
from consumer import Consumer
from bus.topic import Topic
from utils import log_decorator

@log_decorator
def main():
    bus_obj = Bus('kafka', 'cfg')
    prod_obj = Producer(bus_obj)
    topic_obj = Topic(bus_obj)
    # topic_obj.create('new-topic')
    for i in range(10):
        prod_obj.send(topic="new-topic", message=b"test message")
    
    cons_obj = Consumer(bus_obj)
    cons_msg = cons_obj.receive(['new-topic'])
    for each in cons_msg:
        print(each)


if __name__ == "__main__":
    main()



