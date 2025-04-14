from message_brokers.message_broker import MessageBroker

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
