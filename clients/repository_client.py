import datetime
import os
import random
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

    def __init__(self, client_id, broker, logs_dir, fault_probabillity=0.05):
        super().__init__(client_id, broker, logs_dir)
        self.client_id = client_id
        self.fault_prob = fault_probabillity

    def publish(self, channel, message):
        ack_channel = channel + reliable_prefixes.S_ACK
        super().publish(channel + reliable_prefixes.ARCHIVED, message)

    def repo_subscriber_callback(self, channel, message, base):
        fault_score = random.random()

        if (fault_score > self.fault_prob):
            super().log_to_publish_file(f"Repository Sending ACK...")
            super().publish(base + reliable_prefixes.P_ACK,
                            f"Repository ACK for message: \"{message}\"")
            super().publish(base + reliable_prefixes.ARCHIVED,
                            f"Repository forwarded message: \"{message}\"")
        else:
            super().log_to_publish_file(f"Repository Sending PNAK...")
            super().publish(base + reliable_prefixes.P_NAK,
                            f"Repository PNAK for message: \"{message}\"")

    def order_callback(self, channel, message, base):
        super().log_to_publish_file(f"Received P2R-Order...")
        self.repo_subscriber_callback(channel, message, base)

    def retransmit_callback(self, channel, message, base):
        super().log_to_publish_file(f"Received P2R-Retransmit...")
        self.repo_subscriber_callback(channel, message, base)

    def subscribe(self, channel):
        super().log_to_notification_file(f"Subscribed to {channel}")
        super().subscribe(channel + reliable_prefixes.ORDER,
                          lambda channel, message, base=channel: self.order_callback(channel, message, base))
        super().subscribe(channel + reliable_prefixes.P_RETRANSMIT,
                          lambda channel, message, base=channel, self=self: self.retransmit_callback(channel, message, base))
