import pytest
import os, pickle
from util import Uri
import glob
from helpers import read_write_map, read_write_reduce
from master import endpoints, insert_map_tasks, insert_reduce_tasks, q_map, q_reduce
from queue import Queue




def test_insert_map_tasks():
    insert_map_tasks(f"input/test_file", 4, 8)
    assert q_map.counter == 4
    assert q_map.retrieve() == {"file_name": f"input/test_file", "i": 0, "m": 7, "partition_size": 24}

def test_insert_reduce_task():
    insert_reduce_tasks(8)
    
    assert q_reduce.counter == 8
    assert q_reduce.retrieve() == {"index": 0}

def test_endpoints():
    packet = {"uri": Uri.GET_MAP, "data": None}
    data = endpoints(packet)
    data = pickle.loads(data)
    assert data["data"] == {"file_name": f"input/test_file", "i": 1, "m": 7, "partition_size": 24}

def test_read_write_map():
    read_write_map(f"input/test_file", 0, 72, 8)
    
    f = open("intermediate/mr-0-4")
    line = f.readline()
    line = line.split()
    assert line[0] == "This"
    f.close()


def test_read_write_reduce():
    read_write_reduce(4)
    
    f = open("out/out-4")
    line = f.readline()
    line = line.split()
    assert int(line[1]) == 3
    f.close()
    
    # clean up
    for name in glob.glob(f"intermediate/*"):
        os.remove(name)
        
    for name in glob.glob(f"out/*"):
        os.remove(name)


def test_add_retrieve():
    my_queue = Queue()
    data = [1, 2]
    my_queue.add(data)
    data = [3, 4]
    my_queue.add(data)
    
    res = my_queue.retrieve()
    
    assert res == [1, 2]

def test_counter():
    my_queue = Queue()
    data = [1, 2]
    my_queue.add(data)
    data = [3, 4]
    my_queue.add(data)
    
    assert my_queue.counter == 2

def test_requeue():
    my_queue = Queue()
    data = [3, 5]
    my_queue.add(data)
    data = [3, 4]
    my_queue.add(data)
    data = [5, 6]
    my_queue.add(data)
    
    res = my_queue.retrieve()
    res = my_queue.retrieve()
    
    my_queue.requeue(res)
    
    res = my_queue.retrieve()
    
    
    assert res == [3, 4]
    
def test_finished_work():
    my_queue = Queue()
    data = [1, 2]
    my_queue.add(data)
    data = [3, 4]
    my_queue.add(data)
    assert my_queue.is_working == 0
    assert my_queue.is_finished_jobs() == False
    
    _ = my_queue.retrieve()
    _ = my_queue.retrieve()
    
    my_queue.finished_work()
    
    assert my_queue.is_working == 1
    
    my_queue.finished_work()
    
    assert my_queue.is_working == 0
    
    assert my_queue.is_finished_jobs() == True
    

