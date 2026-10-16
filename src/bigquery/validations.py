"""Post-load BigQuery validations (Phase 10, BigQuery section).

Every check degrades to ``SKIPPED`` in dry-run mode so the same code path runs
offline, in CI and against DEV.
"""

from __future__ import annotations

from ..models.pipeline_spec import PipelineSpec, TableSpec
from ..models.validation import ValidationReport, ValidationResult
from ..utils.logging_setup import get_logger
from .table_creator import BigQueryExecutor

logger = get_logger(__name__)


def run_bigquery_validations(
    executor: BigQueryExecutor, spec: PipelineSpec, report: ValidationReport | None = None
) -> ValidationReport:
    report = report or ValidationReport()
    if executor.dry_run:
        for table in spec.tables:
            report.add(
                ValidationResult(
                    f"bq.{table.table_01}", "SKIPPED",
                    "dry run - BigQuery validations not executed",
                )
            )
        return report

    for table in spec.tables:
        _validate_table(executor, spec, table, report)
    logger.info(
        "BigQuery validation: %s checks, %s failed", len(report.results), len(report.failures)
    )
    return report


def _validate_table(
    executor: BigQueryExecutor, spec: PipelineSpec, table: TableSpec, report: ValidationReport
) -> None:
    prefix = f"bq.{table.table_01}"
    fqn_00 = f"`{spec.project_id}.{spec.dataset_00}.{table.table_00}`"
    fqn_01 = f"`{spec.project_id}.{spec.dataset_01}.{table.table_01}`"
    run_date = spec.run_date

    # 1. the 00 external table is queryable
    probe = executor.execute(f"SELECT COUNT(*) AS c FROM {fqn_00};", f"validate_00_{table.table_00}", expects_rows=True)
    report.add_result(
        f"{prefix}.00_queryable", probe.ok,
        f"{table.table_00} is queryable", f"{table.table_00} query failed: {probe.error}",
    )

    # 2. the 01 table exists with the expected partitioning/clustering
    meta_sql = (
        "SELECT ddl FROM "
        f"`{spec.project_id}.{spec.dataset_01}.INFORMATION_SCHEMA.TABLES` "
        f"WHERE table_name = '{table.table_01}';"
    )
    meta = executor.query_rows(meta_sql, f"validate_01_exists_{table.table_01}")
    report.add_result(
        f"{prefix}.01_exists", bool(meta), f"{table.table_01} exists", f"{table.table_01} was not found",
    )
    if meta:
        ddl = (meta[0].get("ddl") or "").upper()
        report.add_result(
            f"{prefix}.partition_field", f"PARTITION BY {table.partition_by.upper()}" in ddl,
            f"partitioned by {table.partition_by}",
            f"{table.table_01} is not partitioned by {table.partition_by}",
        )
        if table.cluster_by:
            expected = ", ".join(table.cluster_by).upper()
            report.add_result(
                f"{prefix}.cluster_fields", f"CLUSTER BY {expected}" in ddl,
                f"clustered by {expected}",
                f"{table.table_01} clustering does not match {expected}",
            )

    # 3. 00 row count for the run equals what landed in 01
    partition_physical = table.physical_name(table.partition_by)
    if partition_physical:
        filter_00 = (
            f"WHERE SAFE_CAST(NULLIF(TRIM({partition_physical}), '') AS DATE) = DATE '{run_date}'"
        )
    else:
        # The staged file has no run column, so all of its rows belong to this run.
        filter_00 = ""
    count_sql = (
        f"SELECT\n"
        f"  (SELECT COUNT(*) FROM {fqn_00}\n"
        f"    {filter_00}) AS rows_00,\n"
        f"  (SELECT COUNT(*) FROM {fqn_01}\n"
        f"    WHERE {table.partition_by} = DATE '{run_date}') AS rows_01;"
    )
    counts = executor.query_rows(count_sql, f"validate_counts_{table.table_01}")
    if counts:
        rows_00, rows_01 = int(counts[0]["rows_00"]), int(counts[0]["rows_01"])
        report.add_result(
            f"{prefix}.rows_gt_zero", rows_01 > 0,
            f"{rows_01} rows loaded for {run_date}", f"no rows in 01 for run_date {run_date}",
            rows_01=rows_01,
        )
        report.add_result(
            f"{prefix}.row_count_match", rows_00 == rows_01,
            f"00 and 01 both hold {rows_01} rows for {run_date}",
            f"row count mismatch for {run_date}: 00={rows_00}, 01={rows_01}",
            rows_00=rows_00, rows_01=rows_01,
        )

    # 4. reruns must not leave duplicates
    key_columns = table.cluster_by or [c.name for c in table.file_columns[:1]]
    if key_columns:
        keys = ", ".join(key_columns)
        dup_sql = (
            f"SELECT COUNT(*) AS duplicate_keys FROM (\n"
            f"  SELECT {keys}, COUNT(*) AS n\n"
            f"  FROM {fqn_01}\n"
            f"  WHERE {table.partition_by} = DATE '{run_date}'\n"
            f"  GROUP BY {keys}\n"
            f"  HAVING COUNT(*) > 1\n"
            f");"
        )
        dupes = executor.query_rows(dup_sql, f"validate_duplicates_{table.table_01}")
        if dupes:
            count = int(dupes[0]["duplicate_keys"])
            report.add_result(
                f"{prefix}.no_duplicate_keys", count == 0,
                f"no duplicate {keys} for {run_date}",
                f"{count} duplicate {keys} group(s) for {run_date} - the rerun did not replace cleanly",
                severity="WARN", duplicate_keys=count,
            )

    # 5. run_date must be exactly the requested run for this partition
    stray_sql = (
        f"SELECT COUNT(*) AS stray FROM {fqn_01}\n"
        f"WHERE {table.partition_by} IS NULL;"
    )
    stray = executor.query_rows(stray_sql, f"validate_null_partition_{table.table_01}")
    if stray:
        count = int(stray[0]["stray"])
        report.add_result(
            f"{prefix}.no_null_run_date", count == 0,
            "no NULL run_date rows", f"{count} row(s) have a NULL {table.partition_by}",
            null_rows=count,
        )


__all__ = ["run_bigquery_validations"]
