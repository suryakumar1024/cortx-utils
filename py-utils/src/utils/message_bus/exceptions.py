
from src.utils.errors import BaseError

KEY_NOT_FOUND_ERROR = 0x1020
CLIENT_NOT_FOUND_ERROR = 0x1021

class ClientNotfoundError(BaseError):
    def __init__(self, desc=None, message_id=None, message_args=None):
        super().__init__(CLIENT_NOT_FOUND_ERROR, 'Exception : Client Not Found Error')


class KeyNotFoundError(BaseError):
    def __init__(self, desc=None, message_id=None, message_args=None):
        super().__init__(KEY_NOT_FOUND_ERROR, 'Exception : Key Not Found Error')
