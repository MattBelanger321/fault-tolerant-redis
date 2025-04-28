from .redis_broker import RedisMessageBroker
from .streams_broker import RedisStreamsBroker
from .rabbitmq_broker import RabbitMQBroker
from .kafka_broker import KafkaBroker

def get_broker(cfg: dict):
    t = cfg["type"]
    if t == "redis":
        return RedisMessageBroker(host=cfg["host"], port=cfg["port"])
    elif t == "redis_streams":
        return RedisStreamsBroker(host=cfg["host"], port=cfg["port"])
    elif t == "rabbitmq":
        return RabbitMQBroker(
            host=cfg["host"],
            port=cfg["port"],
            username=cfg["username"],
            password=cfg["password"],
            vhost=cfg.get("vhost", "/")
        )
    elif t == "kafka":
        return KafkaBroker(
            bootstrap_servers=cfg["bootstrap_servers"]
        )
    else:
        raise ValueError(f"Unsupported broker type: {t}")