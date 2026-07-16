#!/usr/bin/env python
"""Generate sample JSON events and publish to Kafka or write to a file."""
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta

from kafka import KafkaProducer


EVENT_TYPES = ["click", "purchase", "login", "logout", "view"]
STATUSES = ["success", "failure", "pending"]


def random_event() -> dict:
    """Return a single synthetic event."""
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 10000)}",
        "event_type": random.choice(EVENT_TYPES),
        "event_timestamp": (
            datetime.utcnow() - timedelta(seconds=random.randint(0, 300))
        ).isoformat()
        + "Z",
        "value": round(random.uniform(0.0, 1000.0), 2),
        "status": random.choice(STATUSES),
    }


def produce_to_kafka(bootstrap: str, topic: str, rate: float, count: int):
    """Publish events to a Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        for _ in range(count):
            producer.send(topic, random_event())
            if rate > 0:
                time.sleep(1.0 / rate)
        producer.flush()
    finally:
        producer.close()


def write_to_file(path: str, count: int):
    """Write events to a JSONL file."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w") as f:
        for _ in range(count):
            f.write(json.dumps(random_event(), default=str) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample events for the pipeline."
    )
    parser.add_argument(
        "--bootstrap", default="localhost:9092", help="Kafka bootstrap servers"
    )
    parser.add_argument("--topic", default="events", help="Kafka topic")
    parser.add_argument(
        "--output",
        default=None,
        help="Path to JSONL file; if set, writes to file instead of Kafka",
    )
    parser.add_argument("--count", type=int, default=1000, help="Number of events")
    parser.add_argument(
        "--rate", type=float, default=10.0, help="Events per second to Kafka"
    )
    args = parser.parse_args()

    if args.output:
        write_to_file(args.output, args.count)
        print(f"Wrote {args.count} events to {args.output}")
    else:
        produce_to_kafka(args.bootstrap, args.topic, args.rate, args.count)
        print(f"Produced {args.count} events to {args.topic}")


if __name__ == "__main__":
    main()
