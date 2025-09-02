#!/usr/bin/env bash
set -euo pipefail

python -m src.backfill.publish_historical "$@"


