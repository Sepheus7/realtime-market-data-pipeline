## Realtime Market Data Pipeline

End-to-end, portfolio-friendly realtime pipeline:

- Redpanda (Kafka-compatible) for streaming transport
- PySpark Structured Streaming consumer for processing
- Feature engineering: rolling window returns and volatility
- Sinks: DuckDB (default, file-based) and optional TimescaleDB

### Architecture

```mermaid
flowchart LR
  subgraph Prod[Producer]
    P1["synthetic_ticks.py<br/>JSON ticks"]
  end
  subgraph Stream[Streaming Transport]
    K[(Redpanda/Kafka)]
  end
  subgraph Proc[Processing]
    S["Spark Structured Streaming<br/>windowed features"]
  end
  subgraph Sinks
    D[(DuckDB)]
    T[(TimescaleDB)]
  end
  UI[Redpanda Console]

  P1 -->|topic: ticks| K
  K --> S
  S --> D
  S --> T
  K --> UI
```

### Quickstart

1) Prereqs

- Docker Desktop running
- Python 3.11 recommended

2) Start services

```bash
docker compose up -d
```

Services:

- Redpanda listeners:
  - internal (containers): redpanda:9092
  - host (your shell): localhost:29092
- Redpanda Console on <http://localhost:8080>
- TimescaleDB on localhost:5432 (optional sink)

3) Python env

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create .env for host-run clients (adjust as needed)
cat > .env << 'EOF'
KAFKA_BROKERS=localhost:29092
KAFKA_TOPIC_TICKS=ticks
DUCKDB_PATH=./data/pipeline.duckdb
PG_DSN=postgres://postgres:postgres@localhost:5432/market
SPARK_APP_NAME=rtm-pipeline
SPARK_LOG_LEVEL=WARN
EOF
```

4) Run synthetic tick producer

```bash
export KAFKA_BROKERS=localhost:29092
bash scripts/run_producer.sh --symbols AAPL,MSFT,GOOG --tps 50 --jitter-ms 10
```

5) Run PySpark streaming consumer → DuckDB

```bash
export KAFKA_BROKERS=localhost:29092
bash scripts/run_consumer.sh --window 60s --slide 10s --sink duckdb
```

DuckDB database file: `data/pipeline.duckdb`

### Makefile quickstart

```bash
make up && make topic
make producer
make consumer
# optional
make backfill
make clean-db
```

### What it does

- Generates realistic synthetic ticks per symbol (random walk/GBM)
- Streams to Redpanda topic `ticks`
- Spark reads from Kafka, parses JSON, watermarks by event time
- Computes windowed features per symbol:
  - return over window: log(last_price/first_price)
  - volatility: stddev of log-returns within the window
- Writes batch results to DuckDB, optionally TimescaleDB
- Tracks simple end-to-end latency (event_time → sink write time)

### Configuration

Edit `.env` (or pass flags):

- KAFKA_BROKERS=localhost:29092
- KAFKA_TOPIC_TICKS=ticks
- DUCKDB_PATH=data/pipeline.duckdb
- PG_DSN=postgres://postgres:postgres@localhost:5432/market

### Useful URLs

- Redpanda Console: <http://localhost:8080>
- Host Kafka bootstrap: localhost:29092
- TimescaleDB (psql): `psql postgres://postgres:postgres@localhost:5432/market`

### Backfill strategy

- Historical loader publishes past ticks to the same Kafka topic (`ticks`) but preserves the original `event_time_ms` from your CSV.
- The streaming job computes windows by event time. Because `event_time_ms` is preserved, replayed data lands in correct windows.
- To reprocess history from Kafka, run the consumer with `--starting-offsets earliest` so Spark reads from the beginning.
- Watermarking is set to your window size. Late data older than the watermark relative to the max seen event time is dropped. For pure backfill runs (without mixing live), this is fine. For mixed runs, keep event times ordered or run backfill separately.

### Notes

### Useful commands

- Create topic after services up:

```bash
bash scripts/create_topic.sh
```

- Run with Timescale sink:

```bash
bash scripts/run_consumer.sh --sink timescale
```

- Backfill from CSV:

```bash
bash scripts/run_backfill.sh --csv path/to/historical_ticks.csv
```

- Clean, minimal architecture; swappable sinks
- Realistic windowed features in streaming mode
- Clear separation of producer, consumer, features, and sinks

### Samples and extras

- Sample CSV for backfill: `samples/backfill_sample.csv`
  - Run: `bash scripts/run_backfill.sh --csv samples/backfill_sample.csv` or `make backfill`
- Smoke test: `python scripts/smoke_test.py` (validates message schema on `ticks`)
- Notebook: `notebooks/viz_duckdb.ipynb` to plot windowed log returns and volatility from DuckDB
