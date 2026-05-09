import json
from typing import List

from kafka import KafkaConsumer

from config import AppConfig


def poll_image_keys(config: AppConfig, timeout_ms: int = 1000) -> List[str]:
    """Читает из Kafka ключи новых снимков, загруженных в MinIO."""
    consumer = KafkaConsumer(
        config.kafka_topic,
        bootstrap_servers=config.kafka_bootstrap_servers,
        group_id=config.kafka_consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    object_keys = []
    try:
        for message in consumer:
            object_key = message.value.get("object_key")
            if object_key:
                object_keys.append(object_key)
        if object_keys:
            consumer.commit()
    finally:
        consumer.close()

    return object_keys
