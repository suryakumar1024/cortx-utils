from bus.bus_frame import Bus
from producer import Producer
from consumer import Consumer


def main():
    bus_start = Bus('kafka', 'cfg')
    prod_obj = Producer(bus_start)
    for i in range(10):
        prod_obj.send("testing", b"test message")
    cons_obj = Consumer(bus_start)
    che = cons_obj.receive(['testing'])
    for each in cons_obj.receive(['testing']):
        print(each)


if __name__ == "__main__":
    main()



