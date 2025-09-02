import json
import math
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List

import click
from confluent_kafka import Producer
from dotenv import load_dotenv


@dataclass
class SymbolState:
    symbol: str
    price: float
    drift: float
    volatility: float


def load_config() -> Dict[str, str]:
    load_dotenv()
    return {
        "brokers": os.getenv("KAFKA_BROKERS", "localhost:29092"),
        "topic": os.getenv("KAFKA_TOPIC_TICKS", "ticks"),
    }


def create_producer(brokers: str) -> Producer:
    return Producer({
        "bootstrap.servers": brokers,
        "client.id": "synthetic-tick-producer",
        "queue.buffering.max.messages": 100000,
        "compression.type": "lz4",
        "linger.ms": 5,
        "batch.num.messages": 10000,
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 30000,
    })


def delivery_report(err, msg):
    if err is not None:
        # Print minimal error; for portfolio clarity
        print(f"delivery failed: {err}")


def step_price_gbm(state: SymbolState, dt_seconds: float) -> float:
    # Geometric Brownian Motion
    z = random.gauss(0.0, 1.0)
    drift_term = (state.drift - 0.5 * state.volatility ** 2) * dt_seconds
    diffusion_term = state.volatility * math.sqrt(dt_seconds) * z
    state.price *= math.exp(drift_term + diffusion_term)
    return state.price


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def emit_tick(producer: Producer, topic: str, symbol: str, price: float) -> None:
    event_time_ms = now_ms()
    payload = {
        "symbol": symbol,
        "price": price,
        "event_time_ms": event_time_ms,
    }
    producer.produce(topic, json.dumps(payload).encode("utf-8"), callback=delivery_report)


@click.command()
@click.option("--symbols", default="AAPL,MSFT,GOOG", help="Comma-separated list of symbols")
@click.option("--tps", default=50, type=int, help="Total ticks per second across symbols")
@click.option("--jitter-ms", default=5, type=int, help="Inter-tick sleep jitter in ms")
@click.option("--base-price", default=100.0, type=float, help="Initial price")
@click.option("--vol", default=0.2, type=float, help="Annualized volatility (approx)")
@click.option("--drift", default=0.05, type=float, help="Annualized drift (approx)")
def main(symbols: str, tps: int, jitter_ms: int, base_price: float, vol: float, drift: float):
    cfg = load_config()
    topic = cfg["topic"]
    producer = create_producer(cfg["brokers"])

    symbol_list: List[str] = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    states: Dict[str, SymbolState] = {
        s: SymbolState(symbol=s, price=base_price, drift=drift, volatility=vol) for s in symbol_list
    }

    per_symbol_tps = max(1, tps // max(1, len(symbol_list)))
    dt = 1.0 / per_symbol_tps

    print(f"Producing to topic '{topic}' on {cfg['brokers']} | symbols={symbol_list} | tps≈{tps}")
    try:
        last = time.perf_counter()
        while True:
            now = time.perf_counter()
            elapsed = now - last
            if elapsed < dt:
                time.sleep(max(0.0, dt - elapsed))
            last = time.perf_counter()

            for s, st in states.items():
                price = step_price_gbm(st, dt_seconds=dt)
                emit_tick(producer, topic, s, round(price, 4))

            if jitter_ms > 0:
                time.sleep(random.uniform(0, jitter_ms) / 1000.0)

            producer.poll(0)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()


