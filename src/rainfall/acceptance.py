"""Execute BRD analytical examples using temporary BigQuery fixtures only."""

from __future__ import annotations

from google.cloud import bigquery


FIXTURE_SQL = """
CREATE TEMP TABLE fixture_mapping AS
SELECT 'W' AS watershed_id, 'Synthetic watershed' AS watershed_label, 'R' AS region_code,
  station_id, 0.0 AS station_latitude, 0.0 AS station_longitude,
  10.0 AS station_elevation_m, 100.0 AS watershed_area_km2,
  DATE '2026-01-01' AS effective_start_date, CAST(NULL AS DATE) AS effective_end_date,
  'mapping.csv' AS source_file_name, TIMESTAMP '2026-10-15 00:00:00+00' AS ingested_at,
  DATE '2026-10-15' AS run_date
FROM UNNEST(['A', 'B', 'C', 'D', 'E']) station_id
WHERE {mapping_filter};

CREATE TEMP TABLE fixture_observations AS
SELECT station_id, observation_date,
  IF(station_id = 'E', NULL, IF(station_id = 'A', 10.0 * EXTRACT(DAY FROM observation_date), 5.0)) AS rainfall_mm,
  IF(station_id = 'E', NULL, 1.0) AS rainfall_duration_hours,
  IF(station_id = 'E', NULL, 100.0) AS peak_intensity_mm_per_hour,
  IF(station_id = 'E', 'MISSING', 'VALID') AS quality_flag,
  CAST(NULL AS TIMESTAMP) AS source_updated_at,
  'observations.csv' AS source_file_name, TIMESTAMP '2026-10-15 00:00:00+00' AS ingested_at,
  DATE '2026-10-15' AS run_date
FROM UNNEST(['A', 'B', 'C', 'D', 'E']) station_id
CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(DATE '2026-01-01', DATE '2026-01-03')) observation_date
WHERE {observation_filter};

CREATE TEMP TABLE fixture_thresholds AS
SELECT 'W' AS watershed_id, window_days AS rolling_window_days,
  CASE severity_level WHEN 'WATCH' THEN 25.0 WHEN 'WARNING' THEN 50.0 ELSE 70.0 END AS rainfall_threshold_mm,
  80.0 AS minimum_station_coverage_pct, severity_level,
  DATE '2026-01-01' AS valid_from, CAST(NULL AS DATE) AS valid_to,
  'thresholds.csv' AS source_file_name, TIMESTAMP '2026-10-15 00:00:00+00' AS ingested_at,
  DATE '2026-10-15' AS run_date
FROM UNNEST([1, 3, 7]) window_days CROSS JOIN UNNEST(['WATCH', 'WARNING', 'ALERT']) severity_level;
"""


def verify_analytical_examples(client: bigquery.Client, sql: str, project: str, dataset_00: str) -> list[dict]:
    prefix = sql.split("BEGIN TRANSACTION;", 1)[0]
    for table, fixture in (
        ("00_watershed_station_map", "fixture_mapping"),
        ("00_rain_gauge_daily_observations", "fixture_observations"),
        ("00_flood_watch_thresholds", "fixture_thresholds"),
    ):
        prefix = prefix.replace(f"`{project}.{dataset_00}.{table}`", fixture)
    parameters = [
        bigquery.ScalarQueryParameter("run_date", "DATE", "2026-10-15"),
        bigquery.ScalarQueryParameter("transformed_at", "TIMESTAMP", "2026-10-15T00:00:00Z"),
    ]
    results = []
    for name, mapping_filter, observation_filter, assertions in (
        (
            "three_day_60mm_warning_at_exactly_80pct", "TRUE", "TRUE",
            """ASSERT (SELECT rolling_rainfall_mm = 60 AND threshold_category = 'WARNING'
              AND assessment_status = 'ASSESSED' AND station_coverage_pct = 80
              FROM build_assessment WHERE station_id = 'A'
                AND assessment_date = DATE '2026-01-03' AND rolling_window_days = 3)
              AS 'FAILED: 60mm WARNING at exactly 80 percent coverage';""",
        ),
        (
            "missing_day_is_incomplete_not_zero", "TRUE",
            "NOT (station_id = 'A' AND observation_date = DATE '2026-01-02')",
            """ASSERT (SELECT rolling_rainfall_mm IS NULL AND NOT window_complete
              AND usable_observation_days = 2 AND assessment_status = 'INCOMPLETE'
              AND threshold_category = 'NOT_ASSESSED'
              FROM build_assessment WHERE station_id = 'A'
                AND assessment_date = DATE '2026-01-03' AND rolling_window_days = 3)
              AS 'FAILED: absent day must be incomplete';""",
        ),
        (
            "three_of_four_stations_75pct_below_80pct", "station_id != 'D'", "station_id != 'D'",
            """ASSERT (SELECT expected_station_count = 4 AND usable_station_count = 3
              AND station_coverage_pct = 75 FROM build_coverage
              WHERE watershed_id = 'W' AND assessment_date = DATE '2026-01-03')
              AS 'FAILED: three of four coverage';
              ASSERT (SELECT rolling_rainfall_mm = 60 AND assessment_status = 'NOT_ASSESSED'
                AND threshold_category = 'NOT_ASSESSED' FROM build_assessment
                WHERE station_id = 'A' AND assessment_date = DATE '2026-01-03'
                AND rolling_window_days = 3) AS 'FAILED: low coverage gate';""",
        ),
    ):
        fixture = FIXTURE_SQL.format(mapping_filter=mapping_filter, observation_filter=observation_filter)
        job = client.query(fixture + "\n" + prefix + "\n" + assertions, job_config=bigquery.QueryJobConfig(
            query_parameters=parameters, maximum_bytes_billed=1024**3,
            labels={"application": "regional-rainfall-watch", "environment": "dev"},
        ), location="US")
        job.result()
        results.append({"check": name, "status": "PASS", "job_id": job.job_id,
                        "permanent_table_mutations": False})
    return results
