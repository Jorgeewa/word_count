from rabbit_mq import connect
from master import QueueName
import os, sys
import json
from collections import defaultdict
import glob


def reduce_task():
    _, channel = connect(QueueName.REDUCE.name)
    
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        body = json.loads(body)
        index = int(body["index"])
        count = defaultdict(int)
        for name in glob.glob(f"intermediate/*[{index}]"):
            print(name)
            f = open(name, "r")
            data = f.read()
            data = data.split()
            
            for text in data:
                count[text] +=1
                
            f.close()
        file_name = f"out/out-{index}"
        fo = open(file_name, "a+")
        
        
        for k,v in count.items():
            data = f"{k} {v} \n" 
            fo.write(data)
        fo.close()

    channel.basic_consume(QueueName.REDUCE.name, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages in reduce. To exit press CTRL+C')
    channel.start_consuming()
    
    
if __name__ == '__main__':
    try:
        reduce_task()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)