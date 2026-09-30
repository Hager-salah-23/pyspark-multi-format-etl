#!/usr/bin/env bash
# Upload the input datasets to HDFS, at the paths the pipelines read from.
# Usage: bash scripts/load_data_to_hdfs.sh [local_data_dir]   (default: data)
set -euo pipefail

DATA_DIR="${1:-data}"
HDFS_DIR="/user/hadoop/project"

FILES=(
  order_items.csv
  ecommerce_sales_customer_analytics_150k.csv
  product_catalog.csv
  customer_master.csv
  userdata.parquet
  airports.json
)

hdfs dfs -mkdir -p "$HDFS_DIR"

for f in "${FILES[@]}"; do
  if [[ -f "$DATA_DIR/$f" ]]; then
    hdfs dfs -put -f "$DATA_DIR/$f" "$HDFS_DIR/$f"
    echo "Uploaded $f"
  else
    echo "Skipped $f (not found in $DATA_DIR)" >&2
  fi
done

hdfs dfs -ls "$HDFS_DIR"
