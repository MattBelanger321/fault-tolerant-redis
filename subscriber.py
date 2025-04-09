import redis

def main():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe('my-channel')

    print("Subscribed to 'my-channel'. Waiting for messages...")
    for message in pubsub.listen():
        if message['type'] == 'message':
            print(f"Received message: {message['data']}")

if __name__ == '__main__':
    main()