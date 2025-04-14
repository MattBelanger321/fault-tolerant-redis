import abc
import threading
import sys
import time
import redis

from message_brokers.redis_broker import RedisMessageBroker
from client import Client

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
