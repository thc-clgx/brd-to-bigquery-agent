"""Ad-hoc check of what landed in BigQuery."""

import os

from google.cloud import bigquery

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
DATASETS = tuple(
    value for value in (
        os.environ.get("BQ_DATASET_00", ""),
        os.environ.get("BQ_DATASET_01", ""),
    ) if value
)
if not PROJECT or not DATASETS:
    raise SystemExit("Set GOOGLE_CLOUD_PROJECT and at least one BQ_DATASET_00/BQ_DATASET_01.")

client = bigquery.Client(project=PROJECT)

for dataset in DATASETS:
    print(f"== {dataset}")
    for table in client.list_tables(dataset):
        ref = f"{PROJECT}.{dataset}.{table.table_id}"
        try:
            rows = list(client.query(f"SELECT COUNT(*) AS c FROM `{ref}`").result())[0].c
            print(f"   {table.table_id:48} {rows:>8,} rows")
        except Exception as exc:  # noqa: BLE001
            print(f"   {table.table_id:48} ERROR {str(exc)[:80]}")
