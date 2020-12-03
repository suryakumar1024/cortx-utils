
class Singleton(type):
    _obj = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._obj:
            cls._obj[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._obj[cls]