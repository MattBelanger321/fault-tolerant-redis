import json
import time
import threading
import os
import argparse

from clients.reliabie_client import ReliableClient
from clients.repository_client import RepositoryClient
from message_brokers.factory import get_broker

class ConfigurableMessagingSystem:
    def __init__(self, config, broker, output_dir=None):
        self.broker = broker
        self.clients = {}
        self.publisher_threads = []

        # Set up logging directory
        self.output_dir = output_dir or os.getcwd()
        os.makedirs(self.output_dir, exist_ok=True)

        # Load clients from loaded config dict
        self.load_clients(config)

    def load_clients(self, config):
        """Initialize clients from a config dict."""
        for client_config in config.get('clients', []):
            client_id = client_config.get('id')
            if not client_id:
                print("Skipping client with missing ID")
                continue

            log_dir = os.path.join(self.output_dir, client_id)
            if client_id != 'repository':
                client = ReliableClient(client_id, self.broker, log_dir)
            else:
                client = RepositoryClient(client_id, self.broker, log_dir)

            # subscriptions
            for ch in client_config.get('subscribe', []):
                client.subscribe(ch)
                print(f"Client {client_id} subscribed to {ch}")

            # publishers
            for pub in client_config.get('publish', []):
                ch = pub.get('channel')
                msg = pub.get('message', f"Message from {client_id}")
                freq = pub.get('frequency_ms', 5000) / 1000.0
                if ch:
                    t = threading.Thread(
                        target=self._publisher_thread,
                        args=(client_id, ch, msg, freq),
                        daemon=True
                    )
                    self.publisher_threads.append(t)
                    print(f"Client {client_id} will publish to {ch} every {freq}s")
                else:
                    print(f"Skipping publish config for {client_id}")

            self.clients[client_id] = client

        print(f"Configured {len(self.clients)} clients.")

    def _publisher_thread(self, client_id, channel, message, interval):
        client = self.clients[client_id]
        while True:
            client.publish(channel, message)
            time.sleep(interval)

    def start(self):
        print(f"Starting {type(self.broker).__name__} listener...")
        self.broker.start_listener()
        for t in self.publisher_threads:
            t.start()

        try:
            print("System running. Ctrl+C to stop.")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping...")


def main():
    parser = argparse.ArgumentParser(description="Configurable messaging system")
    parser.add_argument('config_file')
    parser.add_argument('broker_type', choices=['redis','rabbitmq','kafka'],
                        help='Select broker defined in config')
    parser.add_argument('-o','--output-dir', default='./logs', help='Logs directory')
    args = parser.parse_args()

    # load full config
    with open(args.config_file) as f:
        cfg = json.load(f)

    # pick broker config
    broker_cfg = cfg.get('brokers', {}).get(args.broker_type)
    if broker_cfg is None:
        raise ValueError(f"Broker '{args.broker_type}' not defined in config.json")
    broker_cfg['type'] = args.broker_type
    broker = get_broker(broker_cfg)

    system = ConfigurableMessagingSystem(cfg, broker, args.output_dir)
    system.start()

if __name__ == '__main__':
    main()
