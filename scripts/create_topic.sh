#!/usr/bin/env bash
set -euo pipefail

# Run rpk inside the redpanda container using the in-cluster address.
CID=$(docker ps --filter name=redpanda -q | head -n1)
if [ -z "${CID}" ]; then
  echo "Redpanda container not found. Start with: docker compose up -d"
  exit 1
fi

TOPIC=${KAFKA_TOPIC_TICKS:-ticks}
BROKERS=${BROKERS:-redpanda:9092}

echo "Waiting for Redpanda to be ready at ${BROKERS}..."
for i in {1..60}; do
  if docker exec "${CID}" rpk cluster info --brokers "${BROKERS}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
  if [ "$i" -eq 60 ]; then
    echo "Redpanda didn't become ready in time"
    exit 1
  fi
done

docker exec "${CID}" rpk topic create "${TOPIC}" -p 6 -r 1 --brokers "${BROKERS}"
docker exec "${CID}" rpk topic list --brokers "${BROKERS}"

