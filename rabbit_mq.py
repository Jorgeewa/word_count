import pika
from typing import Tuple


def connect(queue: str) -> Tuple[any, any]:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    return connection, channel





