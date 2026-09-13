import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

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

    def close(self):
        try:
            if self._channel.is_open:
                self._channel.close()
            if self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Failed to close RabbitMQ connection: {e}") from e

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to send message to RabbitMQ server at {self._host}: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Failed to send message to RabbitMQ server at {self._host}: {e}") from e

    def start_consuming(self, on_message_callback):
        def _on_message(channel, method, properties, body):

            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                requeue = True
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)

            on_message_callback(body, ack, nack)

        try:
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_on_message,
                auto_ack=False
            )

            self._is_consuming = True

            self._channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to start consuming messages from RabbitMQ server at {self._host}: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Failed to start consuming messages from RabbitMQ server at {self._host}: {e}") from e

    def stop_consuming(self):
        if self._is_consuming:
            try:
                self._channel.stop_consuming()
                self._is_consuming = False
            except Exception as e:
                raise MessageMiddlewareDisconnectedError(f"Failed to stop consuming messages from RabbitMQ server at {self._host}: {e}") from e

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
