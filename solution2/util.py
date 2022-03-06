from enum import Enum, auto

class Uri(Enum):
    GET_MAP = auto()
    QUEUE_REDUCE = auto()
    GET_REDUCE = auto()
    FINISHED_MAP = auto()
    FINISHED_REDUCE = auto()
    UNFINISHED_MAP = auto()
    UNFINISHED_REDUCE = auto()
    STATUS_MAP = auto()
    STATUS_REDUCE = auto()
    
    
IP = "127.0.0.1"
PORT = 10000