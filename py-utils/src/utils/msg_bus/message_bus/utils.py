import logging
import time
from datetime import datetime, timedelta

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

def add(messages):
    li = []
    for each in messages:
        to_dict = dict(each._asdict())
        to_dict['value'] += b' added'
        li.append(to_dict)
    return li

def last_one_hour_data(messages):
    li = []
    for each in messages:
        to_dict = dict(each._asdict())
        to_dict['timestamp'] =datetime.fromtimestamp(to_dict['timestamp']/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        last_hour = (datetime.now() - timedelta(hours = 1)).strftime('%Y-%m-%d %H:%M:%S')
        if to_dict['timestamp'] > last_hour:
            li.append(to_dict)
    return li

def check_topic(topic, messages):
    if 'testing' in topic:
        return True
    else:
        return False