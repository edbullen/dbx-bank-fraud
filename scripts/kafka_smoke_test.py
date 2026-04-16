#!/usr/bin/env python3
"""Quick smoke test: produce one message to Kafka, consume it back."""

import json
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / "app" / ".env")

from confluent_kafka import Producer, Consumer, KafkaError

import os

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
API_KEY = os.getenv("KAFKA_API_KEY", "")
API_SECRET = os.getenv("KAFKA_API_SECRET", "")
TOPIC = os.getenv("KAFKA_TOPIC", "fraud-transactions")

if not BOOTSTRAP or not API_KEY:
    print("ERROR: KAFKA_BOOTSTRAP_SERVERS / KAFKA_API_KEY not set in .env")
    sys.exit(1)

SASL_CONFIG = {
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
}

# ── Produce ──
print(f"Producing to topic '{TOPIC}' on {BOOTSTRAP}...")
producer = Producer(SASL_CONFIG)
test_msg = {"test": True, "ts": time.time(), "message": "Hello from fraud demo smoke test"}
producer.produce(TOPIC, json.dumps(test_msg).encode("utf-8"))
producer.flush(timeout=10)
print(f"  Sent: {test_msg}")

# ── Consume ──
print(f"Consuming from topic '{TOPIC}'...")
consumer = Consumer({
    **SASL_CONFIG,
    "group.id": "smoke-test-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

deadline = time.time() + 15
received = False
while time.time() < deadline:
    msg = consumer.poll(timeout=2.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() != KafkaError._PARTITION_EOF:
            print(f"  Consumer error: {msg.error()}")
        continue
    payload = json.loads(msg.value().decode("utf-8"))
    print(f"  Received: {payload}")
    received = True
    break

consumer.close()

if received:
    print("\nSMOKE TEST PASSED -- Kafka produce/consume working!")
else:
    print("\nSMOKE TEST FAILED -- no message received within 15 seconds")
    sys.exit(1)
