from rabbit_mq import connect
from enum import Enum, auto
import json
import traceback
import os

class QueueName(Enum):
    MAP = auto()
    REDUCE = auto()


def insert_map_tasks(file_name: str, n: int, m: int) -> None:
    _, channel = connect(QueueName.MAP.name)
    
    try:
        f = open(file_name, "r")
        size = os.path.getsize(file_name)
        int_n = int(n)
        int_m = int(m)
        partition_size = size // int_n
        for i in range(int_n):
            finish = i == int_n - 1
            data = {"file_name": file_name, "i": i, "m": int_m, "is_last": finish, "partition_size": partition_size}
            channel.basic_publish(exchange='',
                                routing_key=QueueName.MAP.name,
                                body=json.dumps(data))
            print(f" [x] Queued this data for map: {data}")
        
    except:
        error = traceback.format_exc()
        
    finally:
        f.close()
    




def insert_reduce_tasks(index: int) -> None:
    _, channel = connect(QueueName.REDUCE.name)
    data = {"index": int(index)}
    channel.basic_publish(exchange='',
                        routing_key=QueueName.REDUCE.name,
                        body=json.dumps(data))
    print(" [x] Queued data for reduce")