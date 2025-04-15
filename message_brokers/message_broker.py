
import abc


##############################
# Messaging Interface Module #
##############################


class MessageBroker(abc.ABC):
    @abc.abstractmethod
    def unsubscribe(self, channel):
        """Unsubscribe from a channel"""
        pass

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
