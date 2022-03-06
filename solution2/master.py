import threading
import pickle
from util import Uri, IP, PORT
import sys, os
from queue import Queue
import traceback
from multiprocessing.connection import Listener

connections = []
q_map = Queue()
q_reduce = Queue()

def endpoints(data: Uri):
    '''
        This function acts as a router, routing all endpoints to specific data or backend operation needed.

        Returns { "uri": endpoint requested, "data": data needed }

        Parameters
        ----------
        data: { "uri": endpoint requested, "data": data needed for request }

        Returns
        --------
        data: { "uri": endpoint requested, "data": data needed }
    '''
    global q_map
    global q_reduce
    
    if data["uri"] == Uri.GET_MAP:
        data = q_map.retrieve()
        data = {"uri": Uri.GET_MAP, "data": data}
        return pickle.dumps(data)
    
    if data["uri"] == Uri.GET_REDUCE:
        data = q_reduce.retrieve()
        data = {"uri": Uri.GET_REDUCE, "data": data}
        return pickle.dumps(data)
    
    if data["uri"] == Uri.FINISHED_MAP:
        q_map.finished_work()
        data = {"uri": Uri.FINISHED_MAP, "data": None}
        return pickle.dumps(data)
    
    if data["uri"] == Uri.FINISHED_REDUCE:
        q_reduce.finished_work()
        data = {"uri": Uri.FINISHED_REDUCE, "data": None}
        return pickle.dumps(data)
    
    if data["uri"] == Uri.UNFINISHED_MAP:
        q_map.requeue(data["data"])
        data = {"uri": Uri.UNFINISHED_MAP, "data": None}
        return pickle.dumps(data)
    
    if data["uri"] == Uri.UNFINISHED_REDUCE:
        q_reduce.requeue(data["data"])
        data = {"uri": Uri.UNFINISHED_REDUCE, "data": None}
        return pickle.dumps(data)
    
    if data["uri"] == Uri.STATUS_MAP:
        print(q_map.counter, q_map.is_working)
        data = {"uri": Uri.STATUS_MAP, "data": q_map.is_finished_jobs()}
        return pickle.dumps(data)
    
    if data["uri"] == Uri.STATUS_REDUCE:
        data = {"uri": Uri.STATUS_REDUCE, "data": q_reduce.is_finished_jobs(), "map": q_map.is_finished_jobs()}
        return pickle.dumps(data)
    


def handler(c, endpoints):
    global connections
    while True:
        try:
            data = c.recv()
            data = pickle.loads(data)
            packet = endpoints(data)
            c.send(packet)
                
        except EOFError as error:
            error = traceback.format_exc()
            if len(connections) == 0:
                os._exit(0)
            if c in connections:
                connections.remove(c)
            
        

def insert_reduce_tasks(m: int) -> None:
    '''
        This function inserts reduce task into the queue.

        Returns None

        Parameters
        ----------
        index: specific number of reduce to work on

        Returns
        --------
        None
    '''
    global q_reduce
    m_int = int(m)
    
    for i in range(m_int):
        data = {"index": i}
        q_reduce.add(data)
        print(" [x] Queued data for reduce")


def insert_map_tasks(file_name: str, n: str, m: str) -> None:
    '''
        This function inserts map task into the queue.

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
    global q_map
    try:
        f = open(file_name, "r")
        size = os.path.getsize(file_name)
        int_n = int(n)
        int_m = int(m) - 1
        partition_size = (size // int_n)
        for i in range(int_n):
            data = {"file_name": file_name, "i": i, "m": int_m, "partition_size": partition_size}
            q_map.add(data)
            print(f" [x] Queued this data for map: {data}")
        
    except:
        error = traceback.format_exc()
        print(error)
        
    finally:
        f.close()
        

def main(file_name: str, n: str, m: str) -> None:
    sock = Listener((IP, PORT))
    
    insert_map_tasks(file_name, n, m)
    insert_reduce_tasks(m)
    
    while True:
        c = sock.accept()
        Thread = threading.Thread(target=handler, args=(c, endpoints))
        Thread.daemon = True
        Thread.start()
        connections.append(c)
        print(connections)
        if q_reduce.is_finished_jobs():
            print("finished all jobs")
            sys.exit(0)
    
    
    
    
if __name__ == "__main__":
    try:
        file_name = sys.argv[1]
        n = sys.argv[2]
        m = sys.argv[3]
    except:
        print("Please call the file in this way: 'python simple_server file_name n m'")
    main(file_name, n, m)
    
    
    
