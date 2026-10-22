"""Opt-in, billable BigQuery acceptance; permanent tables are read-only in these tests."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from google.cloud import bigquery

from src.brd_parser.document_reader import read_document
from src.rainfall.kpi import compile_kpi_sql, extract_kpi_contracts, kpi_parameters
from src.rainfall.preflight import development_resources

pytestmark = pytest.mark.skipif(
    os.getenv("BRD_RUN_CLOUD_TESTS") != "1", reason="Explicit BRD_RUN_CLOUD_TESTS=1 authorization required.",
)


@pytest.fixture
def deployment():
    root = Path(os.environ["BRD_CLOUD_INPUT_DIR"])
    documents = [path for path in root.glob("*.docx") if not path.name.startswith("~$")]
    assert len(documents) == 1
    document = read_document(documents[0])
    resources = development_resources(document)
    assert os.environ["BRD_CLOUD_TEST_PROJECT"] == resources["Google Cloud project ID"]
    return resources, extract_kpi_contracts(document, resources)


def run(client, sql, parameters=()):
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=list(parameters), maximum_bytes_billed=1024**3,
        labels={"application": "regional-rainfall-watch", "environment": "dev"},
    ), location="US")
    job.result()
    print(f"BigQuery acceptance job: {job.job_id}")


def test_supplied_brd_section_9_7_exact_counts_categories_and_kpi_answers(deployment):
    resources, _ = deployment
    client = bigquery.Client(project=resources["Google Cloud project ID"], location="US")
    prefix = resources["Google Cloud project ID"] + "." + resources["02 dataset"]
    checks = [
        ("daily_counts_and_fallback", """SELECT COUNT(*) = 504
          AND COUNTIF(previous_day_coverage_pct IS NULL) = 42
          AND COUNTIF(minimum_coverage_source = 'SAME_MONTH_FALLBACK') = 12
          AND COUNTIF(minimum_coverage_source = 'UNAVAILABLE') = 0
          AND SUM(assessment_row_count) = 813 AND COUNTIF(coverage_below_minimum_flag) = 51
          FROM daily"""),
        ("quality_counts_and_reliability", """SELECT COUNT(*) = 24
          AND MIN(expected_observation_days) = 12 AND MAX(expected_observation_days) = 12
          AND SUM(received_observation_days) = 271 AND SUM(valid_observation_days) = 257
          AND SUM(estimated_observation_days) = 14 AND SUM(missing_observation_days) = 0
          AND COUNTIF(reporting_reliability_pct = 100 AND data_quality_band = 'GOOD' AND reliable_station_flag) = 7
          AND COUNTIF(reporting_reliability_pct = 91.67 AND data_quality_band = 'FAIR' AND NOT reliable_station_flag) = 17
          FROM quality"""),
        ("exact_reliable_stations", """SELECT STRING_AGG(station_id, ',' ORDER BY station_id) =
          'SYN-GAUGE-018,SYN-GAUGE-019,SYN-GAUGE-020,SYN-GAUGE-021,SYN-GAUGE-022,SYN-GAUGE-023,SYN-GAUGE-024'
          FROM quality WHERE reliable_station_flag"""),
        ("monthly_row_count_and_calendar", """SELECT COUNT(*) = 42
          AND MIN(calendar_days_in_scope) = 12 AND MAX(calendar_days_in_scope) = 12 FROM monthly"""),
        ("proximity_count", "SELECT COUNT(*) = 526 FROM proximity"),
        ("kpi_q1_exact_seven_latest_watersheds", """SELECT COUNT(*) = 7
          AND STRING_AGG(CONCAT(watershed_id, '/', warning_or_alert_station_ids[OFFSET(0)], '/',
            CAST(max_warning_or_alert_rolling_rainfall_mm AS STRING)), ',' ORDER BY watershed_id) =
          'SYN-WATERSHED-03/SYN-GAUGE-017/249.9,SYN-WATERSHED-04/SYN-GAUGE-018/259,SYN-WATERSHED-05/SYN-GAUGE-019/268.1,SYN-WATERSHED-06/SYN-GAUGE-020/277.2,SYN-WATERSHED-08/SYN-GAUGE-022/295.4,SYN-WATERSHED-09/SYN-GAUGE-023/256.2,SYN-WATERSHED-10/SYN-GAUGE-024/265.3'
          AND MIN(warning_station_count) = 1 AND MAX(warning_station_count) = 1
          AND MAX(alert_station_count) = 0
          FROM daily WHERE assessment_date = (SELECT MAX(assessment_date) FROM daily)
          AND rolling_window_days = 7 AND highest_threshold_category IN ('WARNING','ALERT')"""),
        ("kpi_q4_exact_latest_top3", """SELECT STRING_AGG(CONCAT(station_id, '/',
          CAST(pct_of_alert_threshold AS STRING), '/', CAST(gap_to_alert_mm AS STRING)),
          ',' ORDER BY alert_proximity_rank) = 'SYN-GAUGE-022/83.16/59.8,SYN-GAUGE-020/78.37/76.5,SYN-GAUGE-019/75.97/84.8'
          FROM proximity WHERE assessment_date = (SELECT MAX(assessment_date) FROM proximity)
          AND rolling_window_days = 7 AND top_3_flag"""),
        ("exclude_not_assessed_station", """SELECT NOT EXISTS (SELECT 1 FROM proximity
          WHERE station_id = 'SYN-GAUGE-021' AND assessment_date = DATE '2026-10-12'
          AND rolling_window_days = 7)"""),
    ]
    sql = "\n".join(
        f"CREATE TEMP TABLE {alias} AS SELECT * FROM `{prefix}.{name}`;"
        for alias, name in [
            ("daily", "02_watershed_daily_rainfall_status"), ("quality", "02_station_monthly_reporting_quality"),
            ("monthly", "02_watershed_monthly_exceedance_summary"), ("proximity", "02_station_alert_proximity"),
        ]
    )
    for label, query in checks:
        sql += f"\nASSERT ({query}) AS 'FAILED: section97_{label}';"
    for window, expected in {
        1: {"WATCH": 84, "NO_THRESHOLD": 67, "NOT_ASSESSED": 13, "NO_ASSESSMENT_ROW": 4, "WARNING": 0, "ALERT": 0},
        3: {"WATCH": 98, "NO_THRESHOLD": 17, "NOT_ASSESSED": 44, "NO_ASSESSMENT_ROW": 4, "WARNING": 5, "ALERT": 0},
        7: {"WATCH": 25, "NO_THRESHOLD": 1, "NOT_ASSESSED": 107, "NO_ASSESSMENT_ROW": 4, "WARNING": 31, "ALERT": 0},
    }.items():
        for category, count in expected.items():
            sql += (f"\nASSERT (SELECT COUNT(*) FROM daily WHERE rolling_window_days = {window} "
                    f"AND highest_threshold_category = '{category}') = {count} "
                    f"AS 'FAILED: section97_window{window}_{category}';")
    expected_monthly = [
        (2, 0, 0, 10, 0, 0, 2), (2, 0, 0, 9, 1, 0, 2), (2, 2, 0, 8, 0, 0, 2),
        (3, 2, 0, 7, 0, 0, 1), (3, 3, 0, 6, 0, 0, 1), (1, 4, 0, 7, 0, 0, 1),
        (1, 4, 0, 7, 0, 0, 1), (1, 5, 0, 6, 0, 0, 1), (0, 5, 0, 7, 0, 0, 1),
        (0, 6, 0, 6, 0, 0, 1), (1, 0, 0, 10, 0, 1, 1), (5, 0, 0, 6, 0, 1, 1),
        (0, 0, 0, 11, 0, 1, 1), (4, 0, 0, 7, 0, 1, 1),
    ]
    fields = ["watch_days", "warning_days", "alert_days", "not_assessed_days",
              "no_threshold_days", "no_assessment_row_days", "coverage_below_minimum_days"]
    for number, expected in enumerate(expected_monthly, start=1):
        predicate = " AND ".join(f"{field} = {value}" for field, value in zip(fields, expected))
        sql += (f"\nASSERT (SELECT {predicate} FROM monthly WHERE watershed_id = 'SYN-WATERSHED-{number:02}' "
                f"AND rolling_window_days = 7 AND month_start_date = DATE '2026-10-01') "
                f"AS 'FAILED: section97_monthly_watershed_{number:02}';")
        days = 2 if number <= 3 else 1
        coverage = 50 if number <= 10 else 0
        sql += (
            f"\nASSERT (SELECT COUNT(*) = {days} AND MIN(station_coverage_pct) = {coverage} "
            f"AND MAX(station_coverage_pct) = {coverage} FROM daily WHERE watershed_id = 'SYN-WATERSHED-{number:02}' "
            f"AND rolling_window_days = 7 AND coverage_below_minimum_flag) AS 'FAILED: section97_drop_{number:02}';"
        )
    run(client, sql)


@pytest.mark.parametrize("mode", ["scoped_refresh", "dq_blocks_all_four", "atomic_rollback"])
def test_scoped_delete_unaffected_rows_validation_gate_and_atomic_rollback_on_temporary_targets(deployment, mode):
    resources, tables = deployment
    client = bigquery.Client(project=resources["Google Cloud project ID"], location="US")
    sql = ""
    mapping = {}
    for index, table in enumerate(tables):
        target = f"test_target_{index}"
        mapping[table.table_id] = target
        partition = table.partition_by
        key = "station_id" if "station_id" in table.business_keys else "watershed_id"
        day = "2026-10-01" if table.partition_granularity == "MONTH" else "2026-10-12"
        sql += f"\nCREATE TEMP TABLE {target} AS SELECT * FROM `{table.table_id}`;"
        sql += (f"\nINSERT INTO {target} SELECT * REPLACE ('STALE_SCOPED' AS {key}, DATE '{day}' AS {partition}) "
                f"FROM {target} LIMIT 1;")
        sql += (f"\nINSERT INTO {target} SELECT * REPLACE ('KEEP_UNAFFECTED' AS {key}, DATE '2026-09-01' AS {partition}) "
                f"FROM {target} LIMIT 1;")
        sql += (f"\nCREATE TEMP TABLE before_{index} AS SELECT TO_JSON_STRING(t) row_json "
                f"FROM {target} t;")
    pending_id = (
        resources["Google Cloud project ID"] + "." + resources["02 dataset"] + "._pipeline_02_pending_dates"
    )
    mapping[pending_id] = "test_pending"
    sql += "\nCREATE TEMP TABLE test_pending AS SELECT DATE '2026-10-12' assessment_date, 'test' run_id;"
    if mode == "dq_blocks_all_four":
        tables[0].select_sql = (
            "SELECT * REPLACE (CAST(NULL AS STRING) AS watershed_label) FROM (" + tables[0].select_sql + ")"
        )
    script = compile_kpi_sql(tables, resources)
    for identifier, target in mapping.items():
        script = script.replace(f"`{identifier}`", target)
    if mode == "atomic_rollback":
        script = script.replace("COMMIT TRANSACTION;", "ASSERT FALSE AS 'forced_rollback';\nCOMMIT TRANSACTION;")
        script = (
            "BEGIN\n" + script + "\nEXCEPTION WHEN ERROR THEN\n"
            "ASSERT STRPOS(@@error.message, 'forced_rollback') > 0 AS 'Unexpected failure';\n"
            "ROLLBACK TRANSACTION;\nEND;"
        )
    sql += "\n" + script
    for index, table in enumerate(tables):
        target = f"test_target_{index}"
        key = "station_id" if "station_id" in table.business_keys else "watershed_id"
        if mode == "scoped_refresh":
            sql += f"\nASSERT NOT EXISTS (SELECT 1 FROM {target} WHERE {key} = 'STALE_SCOPED') AS 'FAILED: stale scope';"
            sql += (f"\nASSERT (SELECT COUNT(*) FROM {target} WHERE {key} = 'KEEP_UNAFFECTED') = 1 "
                    "AS 'FAILED: unaffected row lost';")
            sql += (
                f"\nASSERT NOT EXISTS (SELECT row_json FROM before_{index} WHERE STRPOS(row_json, 'KEEP_UNAFFECTED') > 0 "
                f"EXCEPT DISTINCT SELECT TO_JSON_STRING(t) FROM {target} t) AS 'FAILED: unaffected row changed';"
                f"\nASSERT NOT EXISTS (SELECT row_json FROM before_{index} WHERE STRPOS(row_json, 'STALE_SCOPED') = 0 "
                f"EXCEPT DISTINCT SELECT TO_JSON_STRING(t) FROM {target} t) AS 'FAILED: unchanged lineage or metadata changed';"
            )
        else:
            sql += (
                f"\nASSERT NOT EXISTS (SELECT row_json FROM before_{index} EXCEPT DISTINCT "
                f"SELECT TO_JSON_STRING(t) FROM {target} t) AS 'FAILED: prior rows changed';"
                f"\nASSERT NOT EXISTS (SELECT TO_JSON_STRING(t) FROM {target} t EXCEPT DISTINCT "
                f"SELECT row_json FROM before_{index}) AS 'FAILED: extra rows published';"
            )
    if mode == "dq_blocks_all_four":
        sql += "\nASSERT EXISTS (SELECT 1 FROM kpi_validation_failures WHERE rule = 'required_fields') AS 'FAILED: missing keyed DQ';"
        sql += "\nASSERT (SELECT COUNT(*) FROM test_pending) = 1 AS 'FAILED: failed scope lost';"
    scope = {"dates": [date(2026, 10, 12), date(2026, 10, 13)], "months": [date(2026, 10, 1)]}
    run(client, sql, kpi_parameters(scope, date.today() + timedelta(days=1), datetime.now(timezone.utc)))
