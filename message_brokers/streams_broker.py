import threading
import time
import uuid
import redis
from .message_broker import MessageBroker

class RedisStreamsBroker(MessageBroker):
    """
    Redis Streams broker optimized for high-throughput:
    - Single consumer-group for all streams to batch reads
    - One background worker thread instead of one per channel
    - Blocking reads with tunable batch size
    """
    def __init__(self, host='localhost', port=6379,
                 group_name=None, read_count=10000, block_ms=5):
        # shared Redis client
        self.redis = redis.Redis(host=host, port=port,
                                 decode_responses=True)
        # unique consumer and group
        self.group = group_name or f"grp:streams"
        self.consumer = f"cons:{uuid.uuid4().hex}"
        # registry of subscribed streams → callbacks
        self._streams = {}
        self._lock = threading.Lock()
        # read parameters
        self._read_count = read_count
        self._block_ms = block_ms
        # stop signal and worker thread
        self._stop_evt = threading.Event()
        self._thread = threading.Thread(target=self._worker,
                                        daemon=True)
        self._thread.start()

    def _ensure_group(self, stream):
        """Create the consumer group on a stream once."""
        try:
            self.redis.xgroup_create(stream,
                                     self.group,
                                     id='$', mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def publish(self, channel, message):
        """Append a message to the given stream."""
        self.redis.xadd(channel,
                        {"data": message},
                        maxlen=10000,
                        approximate=True)

    def subscribe(self, channel, callback=None):
        """
        Register a callback for a stream. The broker will batch across all
        streams in a single xreadgroup call.
        """
        if callback is None:
            return
        with self._lock:
            self._ensure_group(channel)
            self._streams[channel] = callback

    def unsubscribe(self, channel):
        """Stop dispatching messages from the given stream."""
        with self._lock:
            self._streams.pop(channel, None)

    def _worker(self):
        """Background loop: batch-read from all subscribed streams."""
        while not self._stop_evt.is_set():
            with self._lock:
                streams = {s: '>' for s in self._streams}
                callbacks = self._streams.copy()
            if not streams:
                time.sleep(0.1)
                continue
            try:
                resp = self.redis.xreadgroup(
                    groupname=self.group,
                    consumername=self.consumer,
                    streams=streams,
                    count=self._read_count,
                    block=self._block_ms
                )
                if not resp:
                    continue
                # resp: list of (stream, [(id, {field: val}), ...])
                for stream, entries in resp:
                    cb = callbacks.get(stream)
                    if not cb:
                        continue
                    for msg_id, fields in entries:
                        cb(stream, fields['data'])
                        self.redis.xack(stream, self.group, msg_id)
            except Exception:
                # on any error, sleep briefly before retry
                time.sleep(0.1)

    def start_listener(self):
        # no-op: worker already running
        pass

    def close(self, timeout=1.0):
        """Shut down the worker and close the Redis client."""
        self._stop_evt.set()
        self._thread.join(timeout)
        try:
            self.redis.close()
        except:
            pass
