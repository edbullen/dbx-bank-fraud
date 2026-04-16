"""
Kafka producer wrapper for publishing raw transactions to Confluent Cloud.

Uses SASL_SSL with PLAIN mechanism (API key/secret authentication).
Lazy-initialized singleton -- no Kafka connection is made unless Kafka is configured.
"""

import json
import logging
from typing import Optional

from confluent_kafka import Producer

from . import config

log = logging.getLogger(__name__)

_producer: Optional[Producer] = None


def _build_config() -> dict:
    return {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": config.KAFKA_API_KEY,
        "sasl.password": config.KAFKA_API_SECRET,
        "linger.ms": 50,
        "batch.num.messages": 16,
    }


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(_build_config())
        log.info(
            "Kafka producer initialized (bootstrap=%s, topic=%s)",
            config.KAFKA_BOOTSTRAP_SERVERS,
            config.KAFKA_TOPIC,
        )
    return _producer


def _delivery_callback(err, msg):
    if err:
        log.error("Kafka delivery failed: %s", err)


def produce(txn: dict) -> None:
    """Serialize a transaction dict to JSON and send it to the configured Kafka topic."""
    producer = _get_producer()
    payload = json.dumps(txn).encode("utf-8")
    producer.produce(
        config.KAFKA_TOPIC,
        value=payload,
        callback=_delivery_callback,
    )
    producer.poll(0)


def flush(timeout: float = 5.0) -> int:
    """Flush pending messages. Returns the number of messages still in the queue."""
    if _producer is not None:
        return _producer.flush(timeout=timeout)
    return 0


def close() -> None:
    """Flush and release the producer."""
    global _producer
    if _producer is not None:
        _producer.flush(timeout=10.0)
        _producer = None
        log.info("Kafka producer closed")
