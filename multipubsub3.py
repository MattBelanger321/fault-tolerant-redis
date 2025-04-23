#!/usr/bin/env python3
# multipubsub3.py

import json
import time
import threading
import os
from message_brokers.factory import get_broker
from clients.reliabie_client import ReliableClient
from clients.repository_client import RepositoryClient


class ConfigurableMessagingSystem:
    def __init__(self, config_file, output_dir=None):
        # ─── Load config & pick broker ───
        with open(config_file, 'r') as f:
            cfg = json.load(f)

        sel = cfg["selected_broker"]              # "redis" | "rabbitmq" | "kafka"
        broker_cfg = cfg["brokers"][sel]
        broker_cfg["type"] = sel
        self.broker = get_broker(broker_cfg)
        self.clients = {}
        self.publisher_threads = []

        # Set up logging directory
        self.output_dir = output_dir or os.getcwd()
        os.makedirs(self.output_dir, exist_ok=True)

        # Initialize clients from config
        self.load_clients(cfg)

    def load_clients(self, config):
        for client_config in config.get('clients', []):
            client_id = client_config.get('id')
            if not client_id:
                continue

            client_log_dir = os.path.join(self.output_dir, client_id)
            if client_id == "repository":
                client = RepositoryClient(client_id, self.broker, client_log_dir)
            else:
                client = ReliableClient(client_id, self.broker, client_log_dir)

            for channel in client_config.get('subscribe', []):
                client.subscribe(channel)
                print(f"Client {client_id} subscribed to channel: {channel}")

            for pub in client_config.get('publish', []):
                channel = pub.get('channel')
                message = pub.get('message', f"Message from {client_id}")
                frequency_ms = pub.get('frequency_ms', 5000)

                if channel:
                    t = threading.Thread(
                        target=self._publisher_thread,
                        args=(client_id, channel, message, frequency_ms),
                        daemon=True
                    )
                    self.publisher_threads.append(t)
                    print(f"Client {client_id} will publish to {channel} every {frequency_ms/1000.0}s")

            self.clients[client_id] = client

        print(f"Configured {len(self.clients)} clients.")

    def _publisher_thread(self, client_id, channel, message, frequency_ms):
        client = self.clients[client_id]
        sleep_time = frequency_ms / 1000.0
        while True:
            client.publish(channel, message)
            time.sleep(sleep_time)

    def start(self):
        print(f"Starting {self.broker.__class__.__name__} listener...")
        self.broker.start_listener()

        print("Starting publisher threads...")
        for t in self.publisher_threads:
            t.start()

        try:
            print("System running. Ctrl+C to stop.")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="JSON-configured messaging system (broker from config)")
    parser.add_argument("config_file", help="Path to JSON configuration file")
    parser.add_argument("--output-dir", "-o",
                        help="Directory for log files", default="./logs")
    args = parser.parse_args()

    system = ConfigurableMessagingSystem(args.config_file, args.output_dir)
    system.start()


if __name__ == '__main__':
    main()
