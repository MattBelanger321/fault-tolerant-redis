from message_broker import MessageBroker
import threading
import time
import redis

###################################
# Redis Pub/Sub Implementation    #
###################################


class RedisMessageBroker(MessageBroker):
    def __init__(self, host='localhost', port=6379):
        # Publisher connection
        self.publisher = redis.Redis(
            host=host, port=port, decode_responses=True)
        # Subscriber connection
        self.subscriber = redis.Redis(
            host=host, port=port, decode_responses=True)
        self.pubsub = self.subscriber.pubsub()
        # Dictionary to store callbacks for each channel
        self.callbacks = {}  # { channel: [callback, ...] }
        self.listening_thread = None

    def subscribe(self, channel, callback=None):
        if callback:
            if channel not in self.callbacks:
                self.callbacks[channel] = []
            self.callbacks[channel].append(callback)
        # Issue subscribe command even if the channel is already subscribed;
        # Redis will ignore duplicate subscriptions.
        self.pubsub.subscribe(channel)
        # Brief pause to help ensure the subscribe command is processed.
        time.sleep(0.1)

    def publish(self, channel, message):
        self.publisher.publish(channel, message)

    def start_listener(self):
        if self.listening_thread is None:
            def _listen():
                # Use polling loop instead of blocking listen()
                while True:
                    message = self.pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1)
                    if message is not None:
                        # We only care about messages of type 'message'
                        if message['type'] == 'message':
                            ch = message['channel']
                            data = message['data']
                            # Dispatch the message to all callbacks registered for this channel
                            if ch in self.callbacks:
                                for cb in self.callbacks[ch]:
                                    cb(ch, data)
                            else:
                                print(
                                    f"[Global Listener] Channel '{ch}': {data}")
                    # Short sleep to avoid busy-waiting
                    time.sleep(0.001)
            self.listening_thread = threading.Thread(
                target=_listen, daemon=True)
            self.listening_thread.start()
