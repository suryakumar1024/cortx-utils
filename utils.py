import logging
import time

logging.basicConfig(filename='log_file.log', level=logging.DEBUG)

def log_decorator(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        logging.debug(f"{func.__name__} function involked at{start}")
        func(*args, **kwargs)
        end = time.time()
        logging.debug(f"{func.__name__} function ended at{end} total time {end-start}")
    return wrapper

def validate_string(func):
    def wrapper(*args, **kwargs):
        for k, v in kwargs.items():
            print(f"k {k}, v{v}")
    return wrapper