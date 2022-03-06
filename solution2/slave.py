import sys, os
from util import Uri, IP, PORT
import pickle
import threading
import time
from multiprocessing.connection import Client
import traceback
from helpers import read_write_map, read_write_reduce

def map_daemon(sock):
    message = {"uri": Uri.STATUS_MAP, "data": None}
    message = pickle.dumps(message)
    sock.send(message)
    while True:
        try:
            message = sock.recv()
            message = pickle.loads(message)
            if message["uri"] == Uri.STATUS_MAP:
                if message["data"]:
                    print("I mapping tasks completed")
                    break
                else:
                    packet = {"uri": Uri.GET_MAP, "data": None}
                    packet = pickle.dumps(packet)
                    sock.send(packet)
                    
            if message["uri"] == Uri.GET_MAP:
                if message["data"] is not None:
                    
                    read_write_map(message["data"]["file_name"], message["data"]["i"], message["data"]["partition_size"], message["data"]["m"])
                    # Added so more workers can be started
                    time.sleep(3)
                    print("finished one, map")
                    packet = {"uri": Uri.FINISHED_MAP, "data": None}
                    packet = pickle.dumps(packet)
                    sock.send(packet)
                    
                    packet = {"uri": Uri.STATUS_MAP, "data": None}
                    packet = pickle.dumps(packet)
                    sock.send(packet)
                else:
                    packet = {"uri": Uri.STATUS_MAP, "data": None}
                    packet = pickle.dumps(packet)
                    sock.send(packet)
                    
                
        except:
            error = traceback.format_exc()
            print(error)
            print(message)
            packet = {"uri": Uri.UNFINISHED_MAP, "data": message["data"]}
            packet = pickle.dumps(packet)
            sock.send(packet)
            os._exit(0)

def reduce_daemon(sock):
    
    message = {"uri": Uri.STATUS_REDUCE, "data": None}
    message = pickle.dumps(message)
    sock.send(message)
    while True:
        try:
            message = sock.recv()
            message = pickle.loads(message)
            if message["uri"] == Uri.STATUS_REDUCE:
                if message["data"]:
                    print("Reduce tasks completed")
                    break
                elif message["map"]:
                    packet = {"uri": Uri.GET_REDUCE, "data": None}
                    packet = pickle.dumps(packet)
                    sock.send(packet)
                else:
                    time.sleep(1)
                    packet = {"uri": Uri.STATUS_REDUCE, "data": None}
                    packet = pickle.dumps(packet)
                    sock.send(packet)
                    
            if message["uri"] == Uri.GET_REDUCE:
                if message["data"] is not None:
                    
                    read_write_reduce(message["data"]["index"])
                    # Added so more workers can be added
                    time.sleep(3)
                    packet = {"uri": Uri.FINISHED_REDUCE, "data": None}
                    packet = pickle.dumps(packet)
                    sock.send(packet)
                    
                    packet = {"uri": Uri.STATUS_REDUCE, "data": None}
                    packet = pickle.dumps(packet)
                    sock.send(packet)
                else:
                    packet = {"uri": Uri.STATUS_REDUCE, "data": None}
                    packet = pickle.dumps(packet)
                    sock.send(packet)

        except:
            error = traceback.format_exc()
            message = pickle.loads(message)
            print(error)
            print(message)
            packet = {"uri": Uri.UNFINISHED_REDUCE, "data": None}
            packet = pickle.dumps(packet)
            sock.send(packet)
            os.exit(0)

    

def main():
    # This is a trick to keep making a request till the master comes online if it is switched off.
    while True:
        try:
            sock = Client((IP, PORT))
            break
        except:
            time.sleep(5)
            pass
    
    
    map_thread = threading.Thread(target=map_daemon, args=(sock,))
    map_thread.start()
    
    sock = Client((IP, PORT))
    
    reduce_thread = threading.Thread(target=reduce_daemon, args=(sock,))
    reduce_thread.start()
    

        
        
        
if __name__ == "__main__":
    main()
