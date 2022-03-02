from rabbit_mq import connect
from master import QueueName
import os, sys
import json
from collections import defaultdict
import glob
import traceback


def read_write_reduce(index: int) -> None:
    '''
        This function gets all the files with a certain reduce number and does the word count.

        Returns None

        Parameters
        ----------
        index: the reduce number

        Returns
        --------
        None
    '''
    count = defaultdict(int)
    for name in glob.glob(f"intermediate/*[{index}]"):
        try:
            f = open(name, "r")
            data = f.read()
            data = data.split()
            
            for text in data:
                count[text] +=1
        except:
            error = traceback.format_exc()
            print(error)
        finally:
            f.close()
    file_name = f"out/out-{index}"
    try:
        fo = open(file_name, "a+")
    
        for k,v in count.items():
            data = f"{k} {v} \n" 
            fo.write(data)
    except:
        error = traceback.format_exc()
        print(error)
    finally:
        fo.close()

def reduce_task():
    _, channel = connect(QueueName.REDUCE.name)
    
    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)
        body = json.loads(body)
        index = int(body["index"])
        read_write_reduce(index)


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