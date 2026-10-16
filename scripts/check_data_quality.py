"""Ad-hoc data-quality check on the loaded 01 tables."""

import os

from google.cloud import bigquery

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
DATASET = os.environ.get("BQ_DATASET_01", "")
if not PROJECT or not DATASET:
    raise SystemExit("Set GOOGLE_CLOUD_PROJECT and BQ_DATASET_01 before running this legacy WFRS check.")
client = bigquery.Client(project=PROJECT)

TABLES = {
    "01_wfrs_brushfire_changelogs_county_test": "countyFIPSCode",
    "01_wfrs_firebreak_changelogs_county_test": "countyFIPSCode",
    "01_wfrs_drought_changelogs_state_test": "stateCode",
    "01_wfrs_wind_changelogs_state_test": "stateCode",
}

for table, key in TABLES.items():
    ref = f"{PROJECT}.{DATASET}.{table}"
    sql = f"""
    SELECT
      COUNT(*)                                        AS n_rows,
      COUNT(DISTINCT run_date)                        AS runs,
      MIN(run_date)                                   AS run_date,
      COUNTIF(run_date IS NULL)                       AS null_run_date,
      COUNTIF(Source_FileName IS NULL)                AS null_src,
      COUNTIF(STARTS_WITH({key}, '0'))                AS leading_zero_rows,
      MIN({key})                                      AS min_key,
      MIN(LOAD_DATE_01)                               AS load_date
    FROM `{ref}`
    """
    r = list(client.query(sql).result())[0]
    print(f"{table}")
    print(f"   rows={r.n_rows:,}  runs={r.runs}  run_date={r.run_date}  load_date={r.load_date}")
    print(f"   null run_date={r.null_run_date}  null Source_FileName={r.null_src}")
    print(f"   {key}: min={r.min_key!r}  rows starting with '0'={r.leading_zero_rows:,}")

    meta = list(client.query(
        "SELECT ddl FROM `{p}.{d}`.INFORMATION_SCHEMA.TABLES WHERE table_name='{t}'".format(
            p=PROJECT, d=DATASET, t=table)
    ).result())[0].ddl
    part = [l.strip() for l in meta.splitlines() if l.strip().startswith(("PARTITION", "CLUSTER"))]
    print(f"   {' | '.join(part)}\n")
