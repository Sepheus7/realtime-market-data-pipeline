.PHONY: up down topic producer consumer producer-bg consumer-bg stop-bg backfill clean-db clean-logs clean-state reset logs

# Default host listener; override with `make producer KAFKA_BROKERS=...`
KAFKA_BROKERS ?= localhost:29092

up:
	docker compose up -d

down:
	docker compose down

topic:
	bash scripts/create_topic.sh

producer:
	KAFKA_BROKERS=$(KAFKA_BROKERS) bash scripts/run_producer.sh --symbols AAPL,MSFT,GOOG --tps 50

consumer:
	KAFKA_BROKERS=$(KAFKA_BROKERS) bash scripts/run_consumer.sh --window 60s --slide 10s --sink duckdb

# Run in background with logs
producer-bg:
	@mkdir -p logs
	@echo "Starting producer in background (logs/producer.log)"
	KAFKA_BROKERS=$(KAFKA_BROKERS) nohup bash -c 'bash scripts/run_producer.sh --symbols AAPL,MSFT,GOOG --tps 50' > logs/producer.log 2>&1 & echo $$! > logs/producer.pid

consumer-bg:
	@mkdir -p logs
	@echo "Starting consumer in background (logs/consumer.log)"
	KAFKA_BROKERS=$(KAFKA_BROKERS) nohup bash -c 'bash scripts/run_consumer.sh --window 60s --slide 10s --sink duckdb' > logs/consumer.log 2>&1 & echo $$! > logs/consumer.pid

stop-bg:
	@echo "Stopping background producer/consumer if running"
	-@test -f logs/producer.pid && kill `cat logs/producer.pid` 2>/dev/null || true
	-@test -f logs/consumer.pid && kill `cat logs/consumer.pid` 2>/dev/null || true

backfill:
	bash scripts/run_backfill.sh --csv samples/backfill_sample.csv

clean-db:
	rm -f data/pipeline.duckdb

clean-logs:
	rm -rf logs

clean-state:
	rm -f data/pipeline.duckdb
	rm -rf data/checkpoints spark-warehouse metastore_db logs

reset:
	$(MAKE) stop-bg || true
	docker compose down -v
	$(MAKE) clean-state

logs:
	docker compose logs -f --tail=200

.PHONY: ui
ui:
	DUCKDB_PATH=$${DUCKDB_PATH:-data/pipeline.duckdb} streamlit run app/streamlit_app.py


