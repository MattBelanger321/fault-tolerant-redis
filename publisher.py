import redis
import time

def main():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    while True:
        msg = input("Enter message to publish (or 'exit' to quit): ")
        if msg.lower() == 'exit':
            break
        r.publish('my-channel', msg)

if __name__ == '__main__':
    main()