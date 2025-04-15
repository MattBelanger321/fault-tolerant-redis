import datetime
import json
import time
import threading
import os
from clients.reliabie_client import ReliableClient
from clients.repository_client import RepositoryClient
from message_brokers.redis_broker import RedisMessageBroker
from clients.client import LoggingClient


class ConfigurableMessagingSystem:
    def __init__(self, config_file, output_dir=None):
        self.broker = RedisMessageBroker()
        self.clients = {}
        self.publisher_threads = []

        # Set up logging directory
        self.output_dir = output_dir or os.getcwd()
        os.makedirs(self.output_dir, exist_ok=True)

        # Load configuration
        self.load_config(config_file)

    def load_config(self, config_file):
        """Load client configuration from a JSON file."""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)

            # Initialize clients
            for client_config in config.get('clients', []):
                client_id = client_config.get('id')
                if not client_id:
                    print(f"Skipping client with missing ID")
                    continue

                # Create client with logging capability
                client_log_dir = os.path.join(self.output_dir, client_id)
                if client_config.get("id", "") != "repository":
                    client = ReliableClient(
                        client_id, self.broker, client_log_dir)
                else:
                    client = RepositoryClient(
                        client_id, self.broker, client_log_dir)

                # Set up subscriptions
                for channel in client_config.get('subscribe', []):
                    client.subscribe(channel)
                    print(
                        f"Client {client_id} subscribed to channel: {channel}")

                # Set up publishers
                for pub_config in client_config.get('publish', []):
                    channel = pub_config.get('channel')
                    message = pub_config.get(
                        'message', f"Message from {client_id}")
                    # Frequency in milliseconds now
                    # Default: 5000ms (5 seconds)
                    frequency_ms = pub_config.get('frequency_ms', 5000)

                    if channel:
                        # Create a publisher thread for this client-channel combination
                        thread = threading.Thread(
                            target=self._publisher_thread,
                            args=(client_id, channel, message, frequency_ms),
                            daemon=True
                        )
                        self.publisher_threads.append(thread)
                        print(
                            f"Client {client_id} will publish to {channel} every {frequency_ms}ms")
                    else:
                        print(
                            f"Skipping publisher config for {client_id} with missing channel")
                self.clients[client_id] = client

            print(f"Configured {len(self.clients)} clients from {config_file}")

        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Error loading configuration: {e}")
            raise

    def _publisher_thread(self, client_id, channel, message, frequency_ms):
        """Continuously publish messages at the specified frequency in milliseconds."""
        client = self.clients[client_id]
        sleep_time = frequency_ms / 1000.0  # Convert ms to seconds for sleep

        while True:
            client.publish(channel, message)
            time.sleep(sleep_time)

    def start(self):
        """Start the message broker and all publisher threads."""
        print(f"Starting Redis message broker...")
        self.broker.start_listener()

        print(f"Starting publisher threads...")
        for thread in self.publisher_threads:
            thread.start()

        try:
            print(f"System running. Press Ctrl+C to exit.")
            print(
                f"Log files are being created in client-specific directories under: {self.output_dir}")
            # Keep the main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            # The broker and thread cleanup will happen on program exit


def main():
    import sys
    import argparse

    parser = argparse.ArgumentParser(
        description="JSON-configured messaging system")
    parser.add_argument("config_file", help="Path to JSON configuration file")
    parser.add_argument("--output-dir", "-o",
                        help="Directory for log files", default="./logs")

    args = parser.parse_args()

    system = ConfigurableMessagingSystem(args.config_file, args.output_dir)
    system.start()


if __name__ == '__main__':
    main()
