"""Pre-load validation of a :class:`PipelineSpec` (Phase 10, pre-load section)."""

from __future__ import annotations

import re
from datetime import date

from ..models.pipeline_spec import PipelineSpec, TableSpec
from ..models.validation import ValidationReport
from ..utils.config import AppConfig
from ..utils.logging_setup import get_logger

logger = get_logger(__name__)

_BQ_NAME = re.compile(r"^[A-Za-z0-9_]+$")


def validate_spec(spec: PipelineSpec, config: AppConfig | None = None) -> ValidationReport:
    """Run every pre-load check. Nothing here touches GCP."""
    report = ValidationReport()

    report.add_result(
        "spec.project_id", bool(spec.project_id),
        f"project_id = {spec.project_id}", "project_id is missing",
        project_id=spec.project_id,
    )
    report.add_result(
        "spec.gcs_bucket", bool(spec.gcs_bucket),
        f"bucket = {spec.gcs_bucket}", "gcs_bucket is missing",
    )
    report.add_result(
        "spec.dataset_00", bool(spec.dataset_00) and bool(_BQ_NAME.match(spec.dataset_00)),
        f"dataset_00 = {spec.dataset_00}", f"dataset_00 is missing or invalid: {spec.dataset_00!r}",
    )
    report.add_result(
        "spec.dataset_01", bool(spec.dataset_01) and bool(_BQ_NAME.match(spec.dataset_01)),
        f"dataset_01 = {spec.dataset_01}", f"dataset_01 is missing or invalid: {spec.dataset_01!r}",
    )
    report.add_result(
        "spec.datasets_distinct", spec.dataset_00 != spec.dataset_01,
        "00 and 01 datasets are distinct", "dataset_00 and dataset_01 must not be the same dataset",
    )
    report.add_result(
        "spec.run_date", isinstance(spec.run_date, date) and date(1990, 1, 1) <= spec.run_date <= date(2100, 1, 1),
        f"run_date = {spec.run_date}", f"run_date is not a plausible date: {spec.run_date!r}",
        run_date=str(spec.run_date),
    )
    report.add_result(
        "spec.tables_present", bool(spec.tables),
        f"{len(spec.tables)} table(s) in spec", "spec contains no tables",
    )
    report.add_result(
        "spec.rerun_strategy", spec.rerun_strategy in {"replace_run", "append", "truncate_table"},
        f"rerun_strategy = {spec.rerun_strategy}", f"unknown rerun_strategy {spec.rerun_strategy!r}",
    )
    if spec.rerun_strategy == "append":
        report.add_result(
            "spec.rerun_no_duplicates", False,
            "", "rerun_strategy 'append' cannot satisfy the BRD rule 'replace the entire run'",
            severity="WARN",
        )

    if config is not None:
        report.add_result(
            "spec.matches_config_project",
            config.gcp.project_id is None or spec.project_id == config.gcp.project_id,
            "spec project matches config",
            f"spec project {spec.project_id!r} != pinned config project {config.gcp.project_id!r}",
        )
        report.add_result(
            "spec.environment", spec.environment == config.environment,
            f"environment = {spec.environment}",
            f"spec environment {spec.environment!r} != config environment {config.environment!r}",
            severity="WARN",
        )

    seen_01: dict[str, str] = {}
    for table in spec.tables:
        _validate_table(table, spec, report)
        if table.table_01 in seen_01:
            report.add_result(
                f"table.{table.source_name}.unique_01", False, "",
                f"01 table {table.table_01} is already mapped from {seen_01[table.table_01]}",
            )
        seen_01[table.table_01] = table.table_00

    for warning in spec.warnings:
        report.add_result(f"parser.note.{len(report.results)}", False, "", warning, severity="WARN")

    logger.info(
        "Spec validation: %s checks, %s failed, %s warnings",
        len(report.results), len(report.failures), len(report.warnings),
    )
    return report


def _validate_table(table: TableSpec, spec: PipelineSpec, report: ValidationReport) -> None:
    prefix = f"table.{table.table_00}"
    column_names = [c.name for c in table.schema_columns]

    report.add_result(
        f"{prefix}.has_01", bool(table.table_01),
        f"{table.table_00} -> {table.table_01}", f"{table.table_00} has no matching 01 table",
    )
    report.add_result(
        f"{prefix}.has_schema", len(table.schema_columns) > 0,
        f"{len(table.schema_columns)} columns", "table has no schema",
        columns=column_names,
    )
    report.add_result(
        f"{prefix}.has_run_date", "run_date" in column_names,
        "run_date present", "schema is missing the run_date metadata column",
    )
    report.add_result(
        f"{prefix}.has_source_file", "Source_FileName" in column_names,
        "Source_FileName present", "schema is missing the Source_FileName metadata column",
    )
    load_date = [c.name for c in table.schema_columns if c.added_at_load]
    report.add_result(
        f"{prefix}.has_load_date", len(load_date) == 1,
        f"load-date column = {load_date[0] if load_date else None}",
        "01 schema must contain exactly one load-date column (e.g. LOAD_DATE_01)",
        columns=load_date,
    )
    report.add_result(
        f"{prefix}.partition_field", table.partition_by in column_names,
        f"partitioned by {table.partition_by}",
        f"partition field {table.partition_by!r} is not in the schema",
    )
    report.add_result(
        f"{prefix}.partition_is_run_date", table.partition_by == "run_date",
        "partitioned by run_date as required by the BRD",
        f"BRD requires partitioning by run_date, spec says {table.partition_by!r}",
        severity="WARN",
    )
    missing_cluster = [c for c in table.cluster_by if c not in column_names]
    report.add_result(
        f"{prefix}.cluster_fields", not missing_cluster,
        f"clustered by {table.cluster_by or '(none)'}",
        f"clustering fields not in schema: {missing_cluster}",
    )
    report.add_result(
        f"{prefix}.cluster_present", bool(table.cluster_by),
        "clustering configured", "no clustering configured; BRD asks for county/state clustering",
        severity="WARN",
    )

    expected_grain_column = {"county": "countyFIPSCode", "state": "stateCode"}.get(table.grain)
    if expected_grain_column:
        column = next((c for c in table.schema_columns if c.name == expected_grain_column), None)
        report.add_result(
            f"{prefix}.grain_column", column is not None,
            f"{table.grain}-grain column {expected_grain_column} present",
            f"{table.grain}-grain table is missing {expected_grain_column}",
        )
        if column is not None:
            report.add_result(
                f"{prefix}.grain_column_is_string", column.type == "STRING" and column.preserve_leading_zeros,
                f"{expected_grain_column} is STRING with leading zeros preserved",
                f"{expected_grain_column} must be STRING with preserve_leading_zeros=True (got {column.type})",
            )

    report.add_result(
        f"{prefix}.gcs_folder", bool(table.gcs_folder) and not table.gcs_folder.startswith("/"),
        f"gcs folder = {table.gcs_folder}", f"invalid gcs_folder {table.gcs_folder!r}",
    )
    report.add_result(
        f"{prefix}.file_pattern", bool(table.file_pattern),
        f"file pattern = {table.file_pattern}*", "file_pattern is empty; the 00 URI would match every object",
    )
    report.add_result(
        f"{prefix}.uri", table.gcs_uri_wildcard(spec.gcs_bucket).startswith(f"gs://{spec.gcs_bucket}/"),
        f"uri = {table.gcs_uri_wildcard(spec.gcs_bucket)}", "generated GCS URI is malformed",
    )

    bad_names = [n for n in column_names if not _BQ_NAME.match(n)]
    report.add_result(
        f"{prefix}.column_names", not bad_names,
        "all column names are valid BigQuery identifiers", f"invalid column names: {bad_names}",
    )
