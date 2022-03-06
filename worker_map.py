from rabbit_mq import connect
from master import QueueName
import os, sys
import json
import re
import requests
from config import hostname
import time
import traceback


def read_write_map(file_name: str, offset: int, limit: int, m:int, is_last: bool) -> None:
    '''
        This function uses pointers to get to a specific point in the file where the meta data specifies it should work
        on.

        Returns None

        Parameters
        ----------
        file_name: the name of the file to process
        offset: point where to seek from
        limit: end of file partition allocated by meta data
        m: number of possible reduces
        is_last: boolean if it is the last map task scheduled by master

        Returns
        --------
        None
    '''
    try:    
        f = open(file_name, "r")
        f.seek(offset * limit)
        data = f.read(limit)
        line_by_line = data.split()

        for text in line_by_line:
            text = re.sub(r'[^a-zA-Z]', '', text)
            if len(text) == 0:
                continue
            index = ord(text[0]) % m
            file_name = f"intermediate/mr-{offset}-{index}"
            try:
                fo = open(file_name, "a+")
                fo.write(f"{text}\n")
            except:
                error = traceback.format_exc()
                print(error)
            finally:        
                fo.close()
    except:
        error = traceback.format_exc()
    finally:
        f.close()    

def map_task():
    _, channel = connect(QueueName.MAP.name)
    
    def callback(ch, method, properties, body):
        body = json.loads(body)
        print(" [x] Received %r" % body)
        file_name = body['file_name']
        offset = int(body["i"])
        limit = int(body["partition_size"])
        m = int(body["m"])
        is_last = body['is_last']
        read_write_map(file_name, offset, limit, m, is_last)
        
        if is_last:
            time.sleep(5)
            for i in range(m):
                response = requests.get(f"{hostname}/queue-reduce?index={i}")
                print(response)

    channel.basic_consume(QueueName.MAP.name, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages in map. To exit press CTRL+C')
    channel.start_consuming()
    
    
if __name__ == '__main__':
    try:
        map_task()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)