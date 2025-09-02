import json
import os
from datetime import datetime, timezone
from typing import Iterable

import click
from confluent_kafka import Producer
from dotenv import load_dotenv


def load_config():
    load_dotenv()
    return {
        "brokers": os.getenv("KAFKA_BROKERS", "localhost:9092"),
        "topic": os.getenv("KAFKA_TOPIC_TICKS", "ticks"),
    }


def create_producer(brokers: str) -> Producer:
    return Producer({
        "bootstrap.servers": brokers,
        "client.id": "historical-loader",
        "compression.type": "lz4",
        "linger.ms": 5,
        "enable.idempotence": True,
        "acks": "all",
    })


def iter_csv_rows(path: str) -> Iterable[dict]:
    import csv

    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


@click.command()
@click.option("--csv", "csv_path", required=True, help="CSV with columns: symbol,price,event_time_ms")
def main(csv_path: str):
    cfg = load_config()
    p = create_producer(cfg["brokers"])
    topic = cfg["topic"]

    for row in iter_csv_rows(csv_path):
        symbol = row["symbol"].strip().upper()
        price = float(row["price"]) 
        event_time_ms = int(row["event_time_ms"])  # Use historical time
        payload = {
            "symbol": symbol,
            "price": price,
            "event_time_ms": event_time_ms,
        }
        p.produce(topic, json.dumps(payload).encode("utf-8"))
        p.poll(0)

    p.flush()


if __name__ == "__main__":
    main()



