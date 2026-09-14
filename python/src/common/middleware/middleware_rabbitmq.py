import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        """
        Initializes the connection to the RabbitMQ broker and declares
        (or reuses) a durable queue with the given name.

        Args:
            host (str): address of the RabbitMQ server.
            queue_name (str): name of the queue to use.

        Raises:
            MessageMiddlewareDisconnectedError: if the connection cannot
                be established, the channel cannot be created, or the
                queue cannot be declared.
        """
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
        """
        Closes the channel and the connection to RabbitMQ, if open.
        Idempotent: calling close() more than once has no additional
        effect and does not raise.

        Raises:
            MessageMiddlewareCloseError: if an internal error occurs
                while closing the channel or the connection.
        """
        try:
            if self._channel.is_open:
                self._channel.close()
            if self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Failed to close RabbitMQ connection: {e}") from e

    def send(self, message):
        """
        Publishes a message to the queue, using RabbitMQ's default
        exchange (routing_key = queue name). The message is marked
        as persistent (delivery_mode=2).

        Args:
            message (bytes): body of the message to send.

        Raises:
            MessageMiddlewareDisconnectedError: if the connection to
                the middleware was lost.
            MessageMiddlewareMessageError: if an internal error occurs
                while publishing the message.
        """
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
        """
        Starts consuming messages from the queue in manual-ack mode.
        For each message received, invokes
        on_message_callback(message, ack, nack), where ack/nack are
        no-argument functions that confirm or reject (with requeue)
        that specific message.

        This method blocks the current thread until stop_consuming()
        is invoked (typically from within the callback itself).

        Args:
            on_message_callback (Callable[[bytes, Callable, Callable], None]):
                function to invoke for each message received.

        Raises:
            MessageMiddlewareDisconnectedError: if the connection to
                the middleware was lost.
            MessageMiddlewareMessageError: if an internal error occurs
                while consuming messages.
        """
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
        """
        Stops consuming messages started by start_consuming().
        If it was not consuming, has no effect and does not raise.

        Raises:
            MessageMiddlewareDisconnectedError: if the connection to
                the middleware was lost while trying to stop consuming.
        """
        if self._is_consuming:
            try:
                self._channel.stop_consuming()
                self._is_consuming = False
            except Exception as e:
                raise MessageMiddlewareDisconnectedError(f"Failed to stop consuming messages from RabbitMQ server at {self._host}: {e}") from e

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        """
        Initializes the connection to the RabbitMQ broker, declares a
        direct exchange with the given name, and creates the instance's
        own anonymous, exclusive queue, bound to each of the given
        routing_keys.

        Args:
            host (str): address of the RabbitMQ server.
            exchange_name (str): name of the exchange to use.
            routing_keys (list[str]): routing keys this instance
                subscribes to (consuming) or publishes to (sending,
                using the first one in the list).

        Raises:
            MessageMiddlewareDisconnectedError: if the connection cannot
                be established, the channel cannot be created, the
                exchange or exclusive queue cannot be declared, or the
                bindings cannot be created.
        """
        self._host = host
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._is_consuming = False

        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._host))
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ server at {self._host}: {e}") from e

        try:
            self._channel = self._connection.channel()
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to create channel for RabbitMQ server at {self._host}: {e}") from e

        try :
            self._channel.exchange_declare(exchange=self._exchange_name, exchange_type='direct', durable=True)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to declare exchange for RabbitMQ server at {self._host}: {e}") from e

        try:
            result = self._channel.queue_declare(queue='', exclusive=True)
            self._queue_name = result.method.queue
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to declare exclusive queue for RabbitMQ server at {self._host}: {e}") from e
        

        try:
            for key in self._routing_keys:
                self._channel.queue_bind(exchange=self._exchange_name, queue=self._queue_name, routing_key=key)
        except Exception as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to bind queue to exchange for RabbitMQ server at {self._host}: {e}") from e

    def close(self):
        """
        Closes the channel and the connection to RabbitMQ, if open.
        Since the queue is exclusive, RabbitMQ automatically deletes
        it once the connection that created it is closed.

        Raises:
            MessageMiddlewareCloseError: if an internal error occurs
                while closing the channel or the connection.
        """
        try:
            if self._channel.is_open:
                self._channel.close()
            if self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Failed to close RabbitMQ connection: {e}") from e

    def send(self, message):
        """
        Publishes a message to the exchange, using the single routing
        key this instance was initialized with (intended for producer
        usage, with a list containing a single routing key).

        Args:
            message (bytes): body of the message to send.

        Raises:
            MessageMiddlewareDisconnectedError: if the connection to
                the middleware was lost.
            MessageMiddlewareMessageError: if an internal error occurs
                while publishing the message.
        """
        try:
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=self._routing_keys[0],
                body=message
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to send message to RabbitMQ server at {self._host}: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Failed to send message to RabbitMQ server at {self._host}: {e}") from e

    def start_consuming(self, on_message_callback):
        """
        Starts consuming messages from this instance's own queue (bound
        to its routing_keys) in manual-ack mode. For each message
        received, invokes on_message_callback(message, ack, nack).

        This method blocks the current thread until stop_consuming()
        is invoked (typically from within the callback itself).

        Args:
            on_message_callback (Callable[[bytes, Callable, Callable], None]):
                function to invoke for each message received.

        Raises:
            MessageMiddlewareDisconnectedError: if the connection to
                the middleware was lost.
            MessageMiddlewareMessageError: if an internal error occurs
                while consuming messages.
        """
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
            raise MessageMiddlewareDisconnectedError(f"Failed to consume messages from RabbitMQ server at {self._host}: {e}") from e
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Failed to consume messages from RabbitMQ server at {self._host}: {e}") from e

    def stop_consuming(self):
        """
        Stops consuming messages started by start_consuming().
        If it was not consuming, has no effect and does not raise.

        Raises:
            MessageMiddlewareDisconnectedError: if the connection to
                the middleware was lost while trying to stop consuming.
        """
        if self._is_consuming:
            try:
                self._channel.stop_consuming()
                self._is_consuming = False
            except Exception as e:
                raise MessageMiddlewareDisconnectedError(f"Failed to stop consuming messages from RabbitMQ server at {self._host}: {e}") from e