import logging
import time

logging.basicConfig(filename='log_file.log', level=logging.DEBUG)

def log_decorator(func):
    def inner(*args, **kwargs):
        start = time.time()
        logging.debug(f"{func.__name__} function involked at{start}")
        func(*args, **kwargs)
        end = time.time()
        logging.debug(f"{func.__name__} function ended at{end}")
        logging.debug(f"{func.__name__} function ended at{end} total time {end-start}")
    return inner