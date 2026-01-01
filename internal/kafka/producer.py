from kafka import KafkaProducer
import json
from typing import Dict, Any

class KafkaEventProducer:
    def __init__(self, brokers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )

    def send_event(self, event: Dict[str, Any], key: str=None) -> None:
        self.producer.send(self.topic, key=key, value=event)

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()
