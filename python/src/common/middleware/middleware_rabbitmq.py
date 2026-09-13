import pika
import random
import string
from .middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._host = host
        self._queue_name = queue_name

        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host))
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self._host}: {e}") from e
        
        try:
            self._channel = self._connection.channel()
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to create channel for RabbitMQ server at {self._host}: {e}") from e

        try:
            self._channel.queue_declare(queue=self._queue_name, durable=True)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to declare queue for RabbitMQ server at {self._host}: {e}") from e

        self._is_consuming = False
        

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
