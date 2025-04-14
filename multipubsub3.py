import abc
import threading
import sys
import time
import redis

##############################
# Messaging Interface Module #
##############################


class MessageBroker(abc.ABC):
    @abc.abstractmethod
    def subscribe(self, channel, callback=None):
        """Subscribe to a channel and optionally register a callback."""
        pass

    @abc.abstractmethod
    def publish(self, channel, message):
        """Publish a message to a channel."""
        pass

    @abc.abstractmethod
    def start_listener(self):
        """Start the background listener that dispatches messages."""
        pass

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

###################################
# Client Abstraction Module       #
###################################


class Client:
    def __init__(self, client_id, broker: MessageBroker):
        self.client_id = client_id
        self.broker = broker

    def subscribe(self, channel):
        def callback(ch, data):
            print(f"[Client {self.client_id}] Received on {ch}: {data}")
        self.broker.subscribe(channel, callback)
        print(f"Client {self.client_id} subscribed to channel '{channel}'.")

    def publish(self, channel, message):
        self.broker.publish(channel, message)
        print(f"Client {self.client_id} published to channel '{channel}'.")

###################################
# Main Interactive Controller     #
###################################


def main():
    # Instantiate the messaging backend.
    broker = RedisMessageBroker()
    broker.start_listener()

    clients = {}

    print("Multi-Client Modular Messaging Controller")
    print("Commands:")
    print("  create client <id>")
    print("  client <id> subscribe <channel>")
    print("  client <id> publish <channel> <message>")
    print("  exit")

    while True:
        try:
            command = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting...")
            break

        if not command:
            continue

        parts = command.split()
        if parts[0] == "create" and len(parts) == 3 and parts[1] == "client":
            client_id = parts[2]
            if client_id in clients:
                print(f"Client {client_id} already exists.")
            else:
                clients[client_id] = Client(client_id, broker)
                print(f"Client {client_id} created.")
        elif parts[0] == "client" and len(parts) >= 4:
            client_id = parts[1]
            if client_id not in clients:
                print(f"Client {client_id} does not exist.")
                continue

            if parts[2] == "subscribe" and len(parts) == 4:
                channel = parts[3]
                clients[client_id].subscribe(channel)
            elif parts[2] == "publish" and len(parts) >= 5:
                channel = parts[3]
                message = " ".join(parts[4:])
                clients[client_id].publish(channel, message)
            else:
                print("Invalid command. Use 'subscribe' or 'publish'.")
        elif parts[0] == "exit":
            print("Exiting...")
            break
        else:
            print("Unknown command. Please try again.")


if __name__ == '__main__':
    main()
