import datetime
import os
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

    def unsubscribe(self, channel):  # correct spelling
        self.broker.unsubscribe(channel)


class LoggingClient(Client):
    """Extended client with logging capabilities"""

    def __init__(self, client_id, broker, logs_dir):
        super().__init__(client_id, broker)
        self.client_id = client_id

        # Create client-specific log files
        self.logs_dir = logs_dir
        os.makedirs(logs_dir, exist_ok=True)

        self.publish_log_file = os.path.join(
            logs_dir, f"{client_id}_publish.log")
        self.notification_log_file = os.path.join(
            logs_dir, f"{client_id}_notifications.log")

        self._initialize_log_files()

    def _initialize_log_files(self):
        """Create or clear client-specific log files and add headers"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(self.publish_log_file, 'w') as f:
            f.write(
                f"# Publish Log for client '{self.client_id}' - Started at {timestamp}\n")
            f.write(
                "# Format: [timestamp] Published to 'channel': message\n\n")

        with open(self.notification_log_file, 'w') as f:
            f.write(
                f"# Notification Log for client '{self.client_id}' - Started at {timestamp}\n")
            f.write(
                "# Format: [timestamp] Received from 'channel': message\n\n")

    def _log_publish(self, channel, message):
        """Log publish events to this client's publish log file"""
        timestamp = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] Published to channel \"'{channel}'\": message: \"{message}\"\n"
        with open(self.publish_log_file, 'a') as f:
            f.write(log_entry)

    def _log_notification(self, channel, message):
        """Log received messages to this client's notification log file"""
        timestamp = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] Received on channel: '{channel}': message: \"{message}\"\n"
        with open(self.notification_log_file, 'a') as f:
            f.write(log_entry)

    def log_to_notification_file(self, message):
        timestamp = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] {message}\n"
        with open(self.notification_log_file, 'a') as f:
            f.write(log_entry)

    def log_to_publish_file(self, message):
        timestamp = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] {message}\n"
        with open(self.publish_log_file, 'a') as f:
            f.write(log_entry)

    def publish(self, channel, message):
        """Override publish to add logging"""
        super().publish(channel, message)
        self._log_publish(channel, message)

    def message_callback(self, ch, msg, cb=None):
        if cb:
            cb(ch, msg)
        self._log_notification(ch, msg)
        # You can add additional processing here if needed

    def subscribe(self, channel, cb=None):
        # Register with broker for this channel
        self.broker.subscribe(channel, lambda ch,
                              msg, cb=cb: self.message_callback(ch, msg, cb))
