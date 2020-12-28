import json
import os
import sys
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from cortx.utils.schema.payload import Json
from cortx.utils.conf_store import ConfStore,Conf

dir_path = os.path.dirname(os.path.realpath(__file__))
file_path = os.path.join(dir_path, 'test_conf_sample_json.json')
sample_config = Json(file_path).load()


def setup_and_generate_sample_files():
    """ This function will generate all required types of file """
    with open(r'/tmp/file1.json', 'w+') as file:
        json.dump(sample_config, file, indent=2)


conf_store = Conf()


def load_config(index, backend_url):
    """Instantiate and Load Config into constore"""
    # conf_backend = KvStoreFactory.get_instance(backend_url)
    conf_store.load(index, backend_url)
    return conf_store

import ipdb;ipdb.set_trace()
conf_store.init(delim='.')
load_config('sspl_local', 'json:///tmp/file1.json')
result_data = conf_store.get('sspl_local', 'bridge.name')
print(result_data)
