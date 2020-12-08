from cortx.utils.conf_store import ConfStore, KvStore
from cortx.utils.conf_store import KvStorageFactory


def example_function(path):
    # local / global
    # conf = ConfStore('local')
    conf = ConfStore()
    kv_store_backend = KvStorageFactory(path)
    conf_backend = KvStore(kv_store_backend)

    # Other backend examples
    # conf_backend = KvStore(KvStorageFactory('salt://prefix'))
    # conf_backend = KvStore(KvStorageFactory('console://prefix'))

    conf.load(index='ssp', store=conf_backend)
    return conf


conf_1 = example_function('file://etc/cortx/config.json')
conf_2 = example_function('console://prefix')
v1 = conf_1.get('a.b.c')