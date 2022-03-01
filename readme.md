Install and start rabbitmq server (https://www.rabbitmq.com/#getstarted)

Start virtual env with python3 -m venv env

Activate virtual env with source env/bin/activate

Install requirements with pip install -r requirements.txt

Start flask server with: export FLASK_APP=api.py and then flask run

Update the config file host name to point to the one indicated by flask which could be http://127.0.0.1:5000/

Visit the browser and type in http://127.0.0.1:5000/queue-map?fileName=inputs/pg-being_ernest.txt&n=4&m=8

The file name can be anyone of the files in the folder inputs. However before running it each time, delete the files inside intermediate and out.

Open a new terminal go into the parent folder start the venv and do python worker_map (you can open as many as you like)

Open a new terminal go into the parent folder start the venv and do python worker_reduce (you can open as many as you like)

The folders intermediate and out should be populated
