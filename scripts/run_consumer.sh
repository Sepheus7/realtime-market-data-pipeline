#!/usr/bin/env bash
set -euo pipefail

PYTHONUNBUFFERED=1 python -u -m src.consumer.spark_streaming "$@"


