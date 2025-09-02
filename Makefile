.PHONY: up down topic producer consumer backfill clean-db logs

up:
	docker compose up -d

down:
	docker compose down

topic:
	bash scripts/create_topic.sh

producer:
	KAFKA_BROKERS?=localhost:29092
	KAFKA_BROKERS=$${KAFKA_BROKERS} bash scripts/run_producer.sh --symbols AAPL,MSFT,GOOG --tps 50

consumer:
	KAFKA_BROKERS?=localhost:29092
	KAFKA_BROKERS=$${KAFKA_BROKERS} bash scripts/run_consumer.sh --window 60s --slide 10s --sink duckdb

backfill:
	bash scripts/run_backfill.sh --csv samples/backfill_sample.csv

clean-db:
	rm -f data/pipeline.duckdb

logs:
	docker compose logs -f --tail=200


