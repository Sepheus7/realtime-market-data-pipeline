import os
import json
from confluent_kafka import Consumer


def main():
    brokers = os.getenv("KAFKA_BROKERS", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC_TICKS", "ticks")

    conf = {
        "bootstrap.servers": brokers,
        "group.id": "smoke-test",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }
    c = Consumer(conf)
    c.subscribe([topic])

    seen = 0
    while seen < 10:
        msg = c.poll(2.0)
        if msg is None:
            continue
        if msg.error():
            raise RuntimeError(msg.error())
        payload = json.loads(msg.value())
        assert "symbol" in payload and isinstance(payload["symbol"], str)
        assert "price" in payload and isinstance(payload["price"], (int, float))
        assert "event_time_ms" in payload and isinstance(payload["event_time_ms"], int)
        seen += 1
    c.close()
    print(f"Smoke test OK - validated {seen} messages from {topic} on {brokers}")


if __name__ == "__main__":
    main()


