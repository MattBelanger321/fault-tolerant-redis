import datetime
import os
from message_brokers.message_broker import MessageBroker
from clients import reliable_prefixes
from clients.client import LoggingClient

###################################
# Repository Client Module        #
###################################

"""
The repository client is a special client that ensures the reliable delivery of messages. 
"""


class RepositoryClient(LoggingClient):

    def __init__(self, client_id, broker, logs_dir):
        super().__init__(client_id, broker, logs_dir)
        self.client_id = client_id

    def publish(self, channel, message):
        ack_channel = channel + reliable_prefixes.S_ACK
        super().publish(channel + reliable_prefixes.ARCHIVED, message)

    def repo_subscriber_callback(self, channel, message, base):
        super().publish(base + reliable_prefixes.P_ACK,
                        f"Repository ACK for message: \"{message}\"")
        super().publish(base + reliable_prefixes.ARCHIVED,
                        f"Repository forwarded message: \"{message}\"")

    def subscribe(self, channel):
        super().log_to_notification_file(f"Subscribed to {channel}")
        super().subscribe(channel + reliable_prefixes.ORDER,
                          lambda channel, message, base=channel: self.repo_subscriber_callback(channel, message, base))
