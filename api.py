from flask import Flask, request
from master import insert_map_tasks, insert_reduce_tasks

app = Flask(__name__)

@app.route('/queue-map', methods=['GET'])
def queue_map():
    '''
        Expects filename: name of file, n number of map, m number of reduce
    '''
    args = request.args
    print(args)
    insert_map_tasks(args.get("fileName"), args.get("n"), args.get("m"))
    return "<p>Your queue request was successful</p>"


@app.route('/queue-reduce', methods=['GET'])
def queue_reduce():
    '''
        Expects filename: index
    '''
    args = request.args
    print(args)
    insert_reduce_tasks(args.get("index"))
    return "<p>Your queue request was successful</p>"