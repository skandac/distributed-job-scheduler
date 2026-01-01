from kafka import KafkaConsumer
import json
from typing import Dict, Any

class KafkaEventConsumer:
    def __init__(self, brokers: str, topic: str, group_id: str):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=brokers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
        )

    def consume_events(self):
        for message in self.consumer:
            yield message.key, message.value

    def close(self) -> None:
        self.consumer.close()
