from rabbit_mq import connect
from master import QueueName
import os, sys
import json
import re
import requests
from config import hostname
import time


def read_write_map(file_name: str, offset: int, limit: int, m:int) -> None:
    
    f = open(file_name, "r")
    f.seek(int(offset * limit))
    data = f.read(limit)
    line_by_line = data.split()

    for text in line_by_line:
        text = re.sub(r'[^a-zA-Z]', '', text)
        if len(text) == 0:
            continue
        index = ord(text[0]) % m
        file_name = f"intermediate/mr-{offset}-{index}"
        fo = open(file_name, "a+")
        fo.write(f"{text}\n")
        fo.close()
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
        read_write_map(file_name, offset, limit, m)
        
        if body['is_last']:
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