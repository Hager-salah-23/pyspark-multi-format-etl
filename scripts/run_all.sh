#!/usr/bin/env bash
# Run the three pipelines one after another with spark-submit.
# Usage: bash scripts/run_all.sh
set -euo pipefail
cd "$(dirname "$0")/.."

for job in ecommerce_csv_analytics userdata_parquet_etl airports_json_analysis; do
  echo "=== Running ${job}.py ==="
  spark-submit "src/${job}.py"
done

echo "All pipelines finished."
