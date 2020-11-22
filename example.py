from busClient import busClient
from busClient import Producer, Consumer


def main():
    busStart = busClient('kafka')
    prod_obj = Producer(busStart)
    print(prod_obj.send('sampl', b"test message"))
    cons_obj = Consumer(busStart)
    print(cons_obj.receive(['sampl']))


if __name__ == "__main__":
    main()


