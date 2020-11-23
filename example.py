from bus.bus_frame import Bus
from producer import Producer
from consumer import Consumer


def main():
    bus_start = Bus('kafka', 'cfg')
    prod_obj = Producer(bus_start)
    print(prod_obj.send('testing', b"test message"))
    cons_obj = Consumer(bus_start)
    print(cons_obj.receive(['testing']))


if __name__ == "__main__":
    main()



