import os
import json
import re
from typing import Dict

import click
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def load_config() -> Dict[str, str]:
    load_dotenv()
    return {
        "app_name": os.getenv("SPARK_APP_NAME", "rtm-pipeline"),
        "log_level": os.getenv("SPARK_LOG_LEVEL", "WARN"),
        "brokers": os.getenv("KAFKA_BROKERS", "localhost:29092"),
        "topic": os.getenv("KAFKA_TOPIC_TICKS", "ticks"),
        "duckdb_path": os.getenv("DUCKDB_PATH", "./data/pipeline.duckdb"),
        "pg_dsn": os.getenv("PG_DSN", "postgres://postgres:postgres@localhost:5432/market"),
    }


def normalize_duration(duration: str) -> str:
    d = duration.strip().lower()
    # Already in long form
    if any(unit in d for unit in [" second", " minute", " hour", " day"]):
        return d
    m = re.fullmatch(r"(\d+)\s*(ms|s|sec|secs|second|seconds|m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)", d)
    if not m:
        return duration
    value = int(m.group(1))
    unit = m.group(2)
    mapping = {
        "ms": "milliseconds",
        "s": "seconds", "sec": "seconds", "secs": "seconds", "second": "seconds", "seconds": "seconds",
        "m": "minutes", "min": "minutes", "mins": "minutes", "minute": "minutes", "minutes": "minutes",
        "h": "hours", "hr": "hours", "hrs": "hours", "hour": "hours", "hours": "hours",
        "d": "days", "day": "days", "days": "days",
    }
    long_unit = mapping.get(unit, unit)
    return f"{value} {long_unit}"


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )
    return spark


def parse_kafka(df: DataFrame) -> DataFrame:
    schema = T.StructType([
        T.StructField("symbol", T.StringType(), False),
        T.StructField("price", T.DoubleType(), False),
        T.StructField("event_time_ms", T.LongType(), False),
    ])

    parsed = (
        df.select(F.col("value").cast("string").alias("json"))
        .select(F.from_json("json", schema).alias("r"))
        .select("r.*")
        .withColumn("event_time", (F.col("event_time_ms") / 1000.0).cast("timestamp"))
    )
    return parsed


def compute_features(df: DataFrame, window: str, slide: str) -> DataFrame:
    # Windowed by event_time
    windowed = (
        df.withWatermark("event_time", window)
        .groupBy(
            F.window("event_time", windowDuration=window, slideDuration=slide),
            F.col("symbol"),
        )
        .agg(
            F.first("price").alias("first_price"),
            F.max(F.struct(F.col("event_time"), F.col("price"))).alias("_last_row"),
            F.count("price").alias("num_ticks"),
            F.max("event_time_ms").alias("max_event_time_ms"),
        )
        .withColumn("last_price", F.col("_last_row.price"))
        .drop("_last_row")
        .withColumn("log_return", F.log(F.col("last_price") / F.col("first_price")))
    )

    # Approx volatility via intra-window log returns using a window over time ordering
    # Requires expanding data rows; for simplicity, compute stddev over price deltas per window using an approximation
    features = (
        windowed
        .withColumn("return_abs", F.abs(F.col("log_return")))
        .withColumn("volatility", F.col("return_abs") / F.sqrt(F.col("num_ticks") + F.lit(1)))
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("first_price"),
            F.col("last_price"),
            F.col("log_return"),
            F.col("volatility"),
            F.col("num_ticks"),
            F.col("max_event_time_ms"),
        )
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn(
            "latency_ms",
            (
                (F.col("ingest_ts").cast("double") * F.lit(1000.0))
                - F.col("max_event_time_ms").cast("double")
            ).cast("long")
        )
    )
    return features


def foreach_batch_writer_duckdb(duckdb_path: str):
    import duckdb

    def write(batch_df: DataFrame, batch_id: int):
        # Ensure column order matches table schema
        selected = (
            batch_df.select(
                "symbol",
                "window_start",
                "window_end",
                "first_price",
                "last_price",
                "log_return",
                "volatility",
                "num_ticks",
                "max_event_time_ms",
                "ingest_ts",
                "latency_ms",
            )
        )
        pdf = selected.toPandas()
        os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)
        con = duckdb.connect(duckdb_path)
        con.execute(
            """
            create table if not exists features (
              symbol text,
              window_start timestamp,
              window_end timestamp,
              first_price double,
              last_price double,
              log_return double,
              volatility double,
              num_ticks bigint,
              max_event_time_ms bigint,
              ingest_ts timestamp,
              latency_ms bigint
            )
            """
        )
        # Backward-compatible migrations for older DBs
        con.execute("alter table features add column if not exists max_event_time_ms bigint;")
        con.execute("alter table features add column if not exists ingest_ts timestamp;")
        con.execute("alter table features add column if not exists latency_ms bigint;")
        con.register("batch", pdf)
        con.execute(
            """
            insert into features (
              symbol, window_start, window_end, first_price, last_price, log_return, volatility, num_ticks, max_event_time_ms, ingest_ts, latency_ms
            )
            select 
              symbol, window_start, window_end, first_price, last_price, log_return, volatility, num_ticks, max_event_time_ms, ingest_ts, latency_ms 
            from batch
            """
        )
        con.close()

    return write


def foreach_batch_writer_timescale(pg_dsn: str):
    import psycopg2

    def write(batch_df: DataFrame, batch_id: int):
        selected = (
            batch_df.select(
                "symbol",
                "window_start",
                "window_end",
                "first_price",
                "last_price",
                "log_return",
                "volatility",
                "num_ticks",
                "max_event_time_ms",
                "ingest_ts",
                "latency_ms",
            )
        )
        pdf = selected.toPandas()
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    create table if not exists features (
                      symbol text,
                      window_start timestamptz,
                      window_end timestamptz,
                      first_price double precision,
                      last_price double precision,
                      log_return double precision,
                      volatility double precision,
                      num_ticks bigint,
                      max_event_time_ms bigint,
                      ingest_ts timestamptz,
                      latency_ms bigint
                    );
                    """
                )
                # Ensure extension exists
                cur.execute("create extension if not exists timescaledb;")
                # Create hypertable if not exists
                cur.execute(
                    """
                    select create_hypertable('features', 'window_start', if_not_exists => true);
                    """
                )
                # Backward-compatible migrations
                cur.execute("alter table features add column if not exists max_event_time_ms bigint;")
                cur.execute("alter table features add column if not exists ingest_ts timestamptz;")
                cur.execute("alter table features add column if not exists latency_ms bigint;")
                # Bulk insert
                args_str = ",".join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row)).decode("utf-8") for row in pdf.itertuples(index=False, name=None))
                cur.execute(
                    """
                    insert into features (
                      symbol, window_start, window_end, first_price, last_price, log_return, volatility, num_ticks, max_event_time_ms, ingest_ts, latency_ms
                    ) values 
                    """ + args_str
                )
                conn.commit()

    return write


def ensure_duckdb_schema(duckdb_path: str) -> None:
    import os as _os
    import duckdb as _duckdb
    _os.makedirs(_os.path.dirname(duckdb_path), exist_ok=True)
    con = _duckdb.connect(duckdb_path)
    con.execute(
        """
        create table if not exists features (
          symbol text,
          window_start timestamp,
          window_end timestamp,
          first_price double,
          last_price double,
          log_return double,
          volatility double,
          num_ticks bigint,
          max_event_time_ms bigint,
          ingest_ts timestamp,
          latency_ms bigint
        )
        """
    )
    con.close()


def ensure_timescale_schema(pg_dsn: str) -> None:
    import psycopg2 as _psycopg2
    with _psycopg2.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("create extension if not exists timescaledb;")
            cur.execute(
                """
                create table if not exists features (
                  symbol text,
                  window_start timestamptz,
                  window_end timestamptz,
                  first_price double precision,
                  last_price double precision,
                  log_return double precision,
                  volatility double precision,
                  num_ticks bigint,
                  max_event_time_ms bigint,
                  ingest_ts timestamptz,
                  latency_ms bigint
                );
                """
            )
            cur.execute(
                "select create_hypertable('features', 'window_start', if_not_exists => true);"
            )
            conn.commit()


@click.command()
@click.option("--window", default="60 seconds", show_default=True, help="Window size (e.g., 60s, 1 minute)")
@click.option("--slide", default="10 seconds", show_default=True, help="Slide interval (e.g., 10s, 5 seconds)")
@click.option("--starting-offsets", default="latest", show_default=True, type=click.Choice(["earliest", "latest"]))
@click.option("--sink", default="duckdb", show_default=True, type=click.Choice(["duckdb", "timescale"]))
def main(window: str, slide: str, starting_offsets: str, sink: str):
    cfg = load_config()
    spark = build_spark(cfg["app_name"])
    spark.sparkContext.setLogLevel(cfg["log_level"])

    # Ensure sink schema exists up-front (useful before first batch emits)
    if sink == "duckdb":
        ensure_duckdb_schema(cfg["duckdb_path"])
    else:
        ensure_timescale_schema(cfg["pg_dsn"])

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["brokers"])
        .option("subscribe", cfg["topic"])
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = parse_kafka(kafka_df)
    norm_window = normalize_duration(window)
    norm_slide = normalize_duration(slide)
    feats = compute_features(parsed, window=norm_window, slide=norm_slide)

    if sink == "duckdb":
        writer = foreach_batch_writer_duckdb(cfg["duckdb_path"])
    else:
        writer = foreach_batch_writer_timescale(cfg["pg_dsn"])

    query = (
        feats.writeStream.outputMode("update")
        .foreachBatch(writer)
        .option("checkpointLocation", "./data/checkpoints/features")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()


