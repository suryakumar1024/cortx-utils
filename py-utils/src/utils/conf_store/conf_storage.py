
from src.utils.kvstore.kvstore  import KvStore
from src.utils.conf_store.conf_cache import ConfCache
from src.utils.conf_store.conf_type import ConfType

# ConfigStore -> confType and Configuration

# open the file -> reads config and setup the filehandle

# filehandle


class ConfStore:

    def __init__(self, confType):
        '''
        confType - type of the configuration : string
        '''
        self.confType = ConfType(confType)
        self._store = KvStore
        
        # Need to be discussed
        self._conf_cache = ConfCache()
    
    def load(self, index, store, force=False) -> None:
        self._store = store

        # if self._conf_cache.get(index=index):
        if False:
            if force == False:
                raise Exception(f'{index} is already loaded')
        store_data = store.load()
        self._conf_cache.set(index, store_data)
            # self._store.load(self.configurations[index])
        # return self.conf_cache.get(index)

    def get(self, key) -> dict:
        import ipdb; ipdb.set_trace()
        return self._conf_cache.get(key)

    def set(self, index, key, value):
        # self.configurations[index] = {key:value}
        # return self.configurations
        # self.conf_cache
        pass

    def save(self):
        pass
    def backup(self):
        pass
    def copy(self, index1, index2):
        pass
    def Merge(self, index1, index2):
        pass
