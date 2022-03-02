from rabbit_mq import connect
from enum import Enum, auto
import json
import traceback
import os

class QueueName(Enum):
    MAP = auto()
    REDUCE = auto()


def insert_map_tasks(file_name: str, n: int, m: int) -> None:
    '''
        This function inserts map task into rabbit mq.

        Returns None

        Parameters
        ----------
        file_name: the name of the file to process
        n: number of possible maps
        m: number of possible reduces
        
        Notes
        -------------------
        This implementation has the possibility of miscounting n words if it cuts the file on these words.
        One solution could be to read the file and make sure allocated partition number is a non character string.
        However this is not a great idea because it requires holding the http connection whilst reading the file.
        A better solution might be to get the first worker to make sure its last data is a alphabetical character (greedy), get the intermediate
        workers to maker sure its first data works back to the first non alphabetical char(non greedy) and last one as above.
        Then finally the last worker will have to check only its start point.
        The idea for using this approach was to make the workers totally independent of each other.

        Returns
        --------
        None
    '''
    _, channel = connect(QueueName.MAP.name)
    
    try:
        f = open(file_name, "r")
        size = os.path.getsize(file_name)
        int_n = int(n)
        int_m = int(m)
        partition_size = (size // int_n)
        for i in range(int_n):
            finish = i == int_n - 1
            data = {"file_name": file_name, "i": i, "m": int_m, "is_last": finish, "partition_size": partition_size}
            channel.basic_publish(exchange='',
                                routing_key=QueueName.MAP.name,
                                body=json.dumps(data))
            print(f" [x] Queued this data for map: {data}")
        
    except:
        error = traceback.format_exc()
        print(error)
        
    finally:
        f.close()
    

def insert_reduce_tasks(index: int) -> None:
    '''
        This function inserts reduce task into rabbit mq.

        Returns None

        Parameters
        ----------
        index: specific number of reduce to work on

        Returns
        --------
        None
    '''
    _, channel = connect(QueueName.REDUCE.name)
    data = {"index": int(index)}
    channel.basic_publish(exchange='',
                        routing_key=QueueName.REDUCE.name,
                        body=json.dumps(data))
    print(" [x] Queued data for reduce")