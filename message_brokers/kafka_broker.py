import re
import threading
from kafka import KafkaProducer, KafkaConsumer
from .message_broker import MessageBroker

class KafkaBroker(MessageBroker):
    def __init__(self, bootstrap_servers):
        # Producer writes to sanitized topics
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v.encode('utf-8')
        )
        # Map sanitized topic -> original channel
        self._topic_map = {}
        self.consumers = {}

    def _sanitize(self, channel: str) -> str:
        # Replace any char not in A-Za-z0-9._- with '_'
        return re.sub(r'[^A-Za-z0-9._-]', '_', channel)

    def publish(self, channel, message):
        topic = self._sanitize(channel)
        # send to Kafka (auto-create topic if enabled)
        self.producer.send(topic, message)
        self.producer.flush()

    def subscribe(self, channel, callback=None):
        topic = self._sanitize(channel)
        # remember mapping for callbacks
        self._topic_map[topic] = channel

        # create consumer for this topic
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.producer.config['bootstrap_servers'],
            auto_offset_reset='earliest',
            group_id=None,
            value_deserializer=lambda v: v.decode('utf-8')
        )
        self.consumers[topic] = consumer

        def _listen():
            try:
                for msg in consumer:
                    orig_channel = self._topic_map.get(msg.topic, msg.topic)
                    if callback:
                        callback(orig_channel, msg.value)
            except AssertionError:
                # consumer closed, exit thread silently
                pass

        t = threading.Thread(target=_listen, daemon=True)
        t.start()

    def unsubscribe(self, channel):
        topic = self._sanitize(channel)
        if topic in self.consumers:
            self.consumers[topic].close()
            del self.consumers[topic]

    def start_listener(self):
        # KafkaConsumer threads handle polling; no global listener needed
        pass
