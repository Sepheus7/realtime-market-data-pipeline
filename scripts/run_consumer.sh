#!/usr/bin/env bash
set -euo pipefail

python -m src.consumer.spark_streaming "$@"


