This project uses a rabbitmq to simulate distributed computing.

You hit an api endpoint with the number of map tasks and reduce tasks identifying the file to be processed which is available in the folder inputs.

The master-slave architecture is used here.

The master queues the tasks which could be either a map task or a reduce task.

The slaves which are two of them access whatever is available in their queues. You can have as many slaves as possible.

The output is saved in the file system. Below are the instructions to get up and running...


--------------------------------------------------------------------------------------------

Install and start rabbitmq server (https://www.rabbitmq.com/#getstarted)

Start virtual env with "python3 -m venv env"

Activate virtual env with "source env/bin/activate"

Install requirements with "pip install -r requirements.txt"

Start flask server with "export FLASK_APP=api.py" and then "flask run"

Update the config file host name to point to the one indicated by flask which could be http://127.0.0.1:5000

Open a new terminal go into the parent folder start the venv and do "python worker_map" (you can open as many as you like)

Open a new terminal go into the parent folder start the venv and do "python worker_reduce" (you can open as many as you like)

Visit the browser and type in http://127.0.0.1:5000/queue-map?fileName=inputs/pg-being_ernest.txt&n=4&m=8

The file name can be anyone of the files in the folder inputs. However before running it each time, delete the files inside intermediate and out.

The folders intermediate and out should be populated


---------------------------------------------------------------------------------------------------

The version 2 of this application uses sockets for communication between master and slave. It can be found in the folder solution2

Master and slave can be started in any order.

Master: this must be started in the following way "python master.py file_name n m".
It immediately partitions the data needed, adds them to a queue and waits for requests from a worker
When connections and all work is finished, the master shuts down.

Slave: this is started like so: "python slave.py"
You can start as many processes as possible and I added a little delay in the code to reflect this.
The code for now does not respond well when you shut the process down manually.
It handles cases where there is an error in processing and requeues the data that failed during processing.
The slave starts two threads one to handle mapping and the other to handle reducing.
It makes sure the mapping task is finished before it starts the reduction task.
The slave shuts down after processing the reduce task


Queue: The queue is not persisted and this was why I used rabbitmq the first time. If one shuts down the process it is cleared from memory.

Testing: The project can be tested by doing: pytest test.py.
The main functions in the project have been tested.

To run a new test, one should always clear the old data from intermediate and out folders or simply run the test which will do this automatically.
