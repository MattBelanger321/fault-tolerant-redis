#!/usr/bin/env python3
# benchmark.py – outputs console + log file

import time, statistics, pathlib
from datetime import datetime
from clients.repository_client import RepositoryClient
from clients.reliabie_client   import ReliableClient
from message_brokers.streams_broker import RedisStreamsBroker
from message_brokers.redis_broker     import RedisMessageBroker
from message_brokers.rabbitmq_broker  import RabbitMQBroker
from message_brokers.kafka_broker     import KafkaBroker
import clients.client as _cl

# Prevent file logging for the benchmark 
_cl.LoggingClient._initialize_log_files    = lambda self: None
_cl.LoggingClient._log_publish             = lambda *a, **k: None
_cl.LoggingClient._log_notification        = lambda *a, **k: None
_cl.LoggingClient.log_to_notification_file = lambda *a, **k: None
_cl.LoggingClient.log_to_publish_file      = lambda *a, **k: None
# 

LOGFILE = pathlib.Path("benchmark_results.txt")

def bench(label, broker, n=40):
    # Start background listener if the broker needs one (Redis)
    try:
        broker.start_listener() # For Redis
    except AttributeError:
        pass

    repo  = RepositoryClient("repo",  broker, "/tmp") # Skip logging
    bench = ReliableClient ("bench", broker, "/tmp")

    repo.subscribe ("test")      # repo will ACK
    bench.subscribe("test")      # bench waits for ACK

    times = []
    for i in range(n):
        t0 = time.perf_counter()
        bench.publish("test", f"m{i}")   # blocks until ACK arrives
        t1 = time.perf_counter()
        times.append(t1 - t0)

    total   = sum(times)
    thr     = n / total
    avg_ms  = statistics.mean(times) * 1e3

    # Console
    print(f"{label:>10}: {thr:7.0f} msg/s | avg {avg_ms:6.2f} ms")

    # Log file (append)
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with LOGFILE.open("a") as fp:
        fp.write(f"{ts}\t{label}\t{n}\t{thr:.0f}\t{avg_ms:.2f}\n")

    if hasattr(broker, "close"):
        try:
            broker.close()            # flush producer, close consumers
        except Exception:
            pass                      # swallow any shutdown errors        

def factory(kind):
    if kind == "redis":
        return RedisMessageBroker()
    if kind == "streams":
        return RedisStreamsBroker()
    if kind == "rabbit":
        return RabbitMQBroker(
            host="localhost", port=5672,
            username="guest", password="guest", vhost="/"
        )
    if kind == "kafka":
        return KafkaBroker(bootstrap_servers=["localhost:9092"])
    raise ValueError(kind)


if not LOGFILE.exists():
    LOGFILE.write_text("utc_timestamp\tbackend\tmessages\tmsg_per_s\tavg_ms\n")

# run benchmarks 
for name in ("redis", "streams", "rabbit", "kafka"):
    print(f"Benchmarking {name} …")
    bench(name, factory(name))
