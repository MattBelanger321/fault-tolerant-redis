from email import message
import threading
from clients.client import LoggingClient
from clients import reliable_prefixes


class ReliableClient(LoggingClient):

    def __init__(self, client_id, broker, logs_dir):
        super().__init__(client_id, broker, logs_dir)
        self.client_id = client_id

    def on_pnak(self, channel, message, base, base_msg):
        super().publish(base + reliable_prefixes.P_RETRANSMIT, base_msg)

    def handle_ack(self, channel, message, ack_event, base_msg):
        super().log_to_notification_file(
            f"Received ACK from Repository for message: \"{base_msg}\"")
        ack_event.set()

    def publish(self, channel, message):
        ack_event = threading.Event()

        nak_channel = channel + reliable_prefixes.P_NAK
        ack_channel = channel + reliable_prefixes.P_ACK

        super().subscribe(nak_channel, lambda channel,
                          message, base=channel, base_msg=message: self.on_pnak(channel, message, base, base_msg))
        super().subscribe(ack_channel, lambda channel, message,
                          self=self, ack_event=ack_event, base_msg=message: self.handle_ack(channel, message, ack_event, base_msg))

        # Logging-enhanced publish
        super().publish(channel + reliable_prefixes.ORDER, message)

        # Wait for ACK
        ack_event.wait()

        # Clean up
        super().unsubscribe(nak_channel)
        super().unsubscribe(ack_channel)

    def reliable_subscriber_callback(self, channel, message, base):
        super().log_to_notification_file(base)
        super().publish(base + reliable_prefixes.S_ACK,
                        f"ACK from {self.client_id} to Repository for message \"{message}\"")

    def subscribe(self, channel):
        super().log_to_notification_file(f"Subscribed to {channel}")
        super().subscribe(channel + reliable_prefixes.ARCHIVED,
                          lambda channel, message, base=channel, self=self: self.reliable_subscriber_callback(channel, message, base))
