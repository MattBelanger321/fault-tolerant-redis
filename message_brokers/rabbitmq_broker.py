import threading
import pika
from .message_broker import MessageBroker

class RabbitMQBroker(MessageBroker):
    def __init__(
        self,
        host='localhost',
        port=5672,
        username='guest',
        password='guest',
        vhost='/'
    ):
        """
        RabbitMQ pub/sub via fanout exchanges, with thread-safe publishing.
        """
        creds = pika.PlainCredentials(username, password)
        self.params = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=vhost,
            credentials=creds
        )
        # Shared publisher connection and channel
        self.pub_conn = pika.BlockingConnection(self.params)
        self.pub_ch = self.pub_conn.channel()
        # Lock to synchronize publish calls
        self._pub_lock = threading.Lock()
        # Track subscriber threads
        self.threads = {}

    def publish(self, channel, message):
        """
        Declare a durable fanout exchange and publish a persistent message under a lock.
        """
        with self._pub_lock:
            self.pub_ch.exchange_declare(
                exchange=channel,
                exchange_type='fanout',
                durable=True
            )
            self.pub_ch.basic_publish(
                exchange=channel,
                routing_key='',
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )

    def subscribe(self, channel, callback=None):
        """
        Subscribe by creating an exclusive auto-deleted queue bound to a durable fanout exchange.
        Blocks until queue is ready to receive messages.
        """
        ready = threading.Event()

        def _consume():
            try:
                conn = pika.BlockingConnection(self.params)
                ch = conn.channel()
                # Ensure exchange exists
                ch.exchange_declare(
                    exchange=channel,
                    exchange_type='fanout',
                    durable=True
                )
                # Create a new exclusive queue
                result = ch.queue_declare(queue='', exclusive=True)
                queue_name = result.method.queue
                # Bind queue to the exchange
                ch.queue_bind(exchange=channel, queue=queue_name)
                # Queue is ready
                ready.set()

                def _on_message(ch_, method, props, body):
                    if callback:
                        callback(channel, body.decode())

                ch.basic_consume(
                    queue=queue_name,
                    on_message_callback=_on_message,
                    auto_ack=True
                )
                ch.start_consuming()
            except pika.exceptions.AMQPError:
                pass

        t = threading.Thread(target=_consume, daemon=True)
        t.start()
        # Wait for subscription setup
        ready.wait()
        self.threads[channel] = t

    def unsubscribe(self, channel):
        """
        No-op: subscriber queues auto-delete on connection close.
        """
        pass

    def start_listener(self):
        """
        No global listener: each subscriber thread consumes independently.
        """
        pass
