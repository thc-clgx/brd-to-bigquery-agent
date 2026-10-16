from __future__ import annotations

import json
from datetime import date
from unittest.mock import Mock

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import bigquery

from src.brd_parser.document_reader import read_document
from src.llm.provider import EchoProvider
from src.rainfall.execution_plan import (
    ExecutionPlan, OPERATIONS, compile_transform_sql, validate_execution_plan,
)
from src.rainfall.executor import (
    ContractBlocked, checked_job, ensure_object, execute, field_schema, object_path, schema_actions, typed_record,
    verify_unchanged_retry,
)
from src.rainfall.preflight import extract_curated_contracts
from src.rainfall.processing import extract_pipeline_contract
from src.rainfall.preflight import authentication_guidance
from src.rainfall.runner import DEFAULT_BRD


@pytest.fixture
def contract(rainfall_brd_available):
    return extract_pipeline_contract(
        rainfall_brd_available, run_date=date(2026, 10, 15), run_id="b" * 20,
        idempotency_key="c" * 64, ingested_at_utc="2026-10-15T00:00:00Z",
        project_id="clgx-surveillance-dev-9e5e", location="US",
        gcs_bucket="regional-rainfall-watch-dev", dataset_00="rainfall_00_monitoring",
        dataset_00_is_approved=True, llm_provider=None, llm_model=None,
        llm_credential_configured=False,
    )[0]


def test_transform_compiler_validates_all_tables_before_atomic_merge(contract, rainfall_brd_available):
    curated = extract_curated_contracts(read_document(rainfall_brd_available))
    sql = compile_transform_sql(contract, curated, "rainfall_01_monitoring")
    assert sql.count("MERGE `") == 4
    assert sql.index("AS 'FAILED: daily reconciliation'") < sql.index("BEGIN TRANSACTION")
    assert sql.index("BEGIN TRANSACTION") < sql.index("MERGE `") < sql.index("COMMIT TRANSACTION")
    assert "CREATE OR REPLACE" not in sql
    assert "DELETE FROM" not in sql
    assert "TRUNCATE" not in sql
    assert "WRITE_TRUNCATE" not in sql
    assert "DATE_SUB(a.observation_date, INTERVAL (window_days - 1) DAY)" in sql
    assert "w.rolling_rainfall_mm >= t.warning_threshold_mm" in sql
    assert "c.station_coverage_pct < t.minimum_station_coverage_pct" in sql
    assert "TO_JSON_STRING(STRUCT(" in sql
    assert "{{" not in sql
    assert "CAST(SUM(CAST(o.rainfall_mm AS BIGNUMERIC)) AS FLOAT64)" in sql
    assert "SUM(o.rainfall_mm)" not in sql
    assert "rainfall exceeds deterministic accumulation range" in sql


def test_llm_plan_rejects_fabricated_evidence_and_dependency_order(contract, rainfall_brd_available):
    document = read_document(rainfall_brd_available)
    curated = extract_curated_contracts(document)
    raw = [f"{contract.project_id}.{contract.dataset_00}.{feed.table_00}" for feed in contract.feeds]
    steps = []
    for name, (operation, _, keys, partition, cluster) in OPERATIONS.items():
        steps.append({
            "table_id": next(table.table_id for table in curated if table.table_id.endswith("." + name)),
            "operation": operation, "business_keys": keys, "partition_by": partition,
            "cluster_by": cluster, "dependencies": raw,
            "evidence_quote": "Create the following four native managed BigQuery tables in dataset rainfall_01_monitoring.",
        })
    plan = ExecutionPlan(summary="test", steps=steps, unsupported_requirements=[], unresolved_decisions=[])
    validate_execution_plan(plan, document, contract, curated)
    plan.steps[0].evidence_quote = "This sentence is invented by the test and is not in the contract."
    with pytest.raises(ValueError, match="evidence"):
        validate_execution_plan(plan, document, contract, curated)
    plan.steps[0].dependencies = ["foreign.dataset.table"]
    with pytest.raises(ValueError, match="dependencies"):
        validate_execution_plan(plan, document, contract, curated)


def test_native_schema_preserves_historical_columns_and_blocks_changes():
    existing = bigquery.Table("project.dataset.00_test", schema=[
        bigquery.SchemaField("key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("historical", "STRING"),
    ])
    existing = bigquery.Table.from_api_repr({**existing.to_api_repr(), "type": "TABLE"})
    wanted = bigquery.Table("project.dataset.00_test", schema=[
        bigquery.SchemaField("key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("new_field", "INT64"),
    ])
    assert [field.name for field in schema_actions(existing, wanted, allow_nullable_additions=True)] == ["new_field"]
    assert [field.name for field in existing.schema] == ["key", "historical"]
    with pytest.raises(ContractBlocked, match="addition"):
        schema_actions(existing, wanted, allow_nullable_additions=False)
    wanted.schema = [bigquery.SchemaField("key", "INT64", mode="REQUIRED")]
    with pytest.raises(ContractBlocked, match="type/nullability"):
        schema_actions(existing, wanted, allow_nullable_additions=True)


def test_source_paths_match_brd_registration():
    day = date(2026, 10, 15)
    assert object_path("rain_gauge_daily_observations", "regional_rainfall_daily_observations.csv", day) == (
        "rainfall_observations/2026/10/15/regional_rainfall_daily_observations.csv"
    )
    assert object_path("watershed_station_map", "regional_watershed_station_map.csv", day) == (
        "watershed_station_map/snapshot_date=2026-10-15/regional_watershed_station_map.csv"
    )
    with pytest.raises(ContractBlocked):
        object_path("watershed_station_map", "unexpected.csv", day)


def test_repeated_source_upload_never_overwrites_different_content():
    bucket = Mock(name="bucket")
    bucket.name = "bucket"
    blob = bucket.blob.return_value
    blob.generation = "12"
    blob.upload_from_string.side_effect = PreconditionFailed("exists")
    blob.download_as_bytes.return_value = b"same"
    result = ensure_object(bucket, "feed/file.csv", b"same", content_type="text/csv")
    assert result["action"] == "REUSED_IDENTICAL"
    blob.upload_from_string.assert_called_once_with(b"same", content_type="text/csv", if_generation_match=0)
    with pytest.raises(ContractBlocked, match="overwrite blocked"):
        ensure_object(bucket, "feed/file.csv", b"different", content_type="text/csv")


def test_load_job_retry_reuses_success_without_duplicate_append():
    client, submit = Mock(), Mock()
    client.get_job.return_value.error_result = None
    _, reused = checked_job(client, "stable_job", submit)
    assert reused
    submit.assert_not_called()
    client.get_job.side_effect = NotFound("not found")
    submit.return_value.error_result = None
    _, reused = checked_job(client, "new_job", submit)
    assert not reused
    submit.assert_called_once()


def test_authentication_failure_writes_actionable_report_before_llm_or_cloud(tmp_path, monkeypatch):
    monkeypatch.setattr("src.rainfall.executor.verify_workload_identity", lambda: {
        "status": "BLOCKED", "reason": "ADC_NOT_CONFIGURED",
    })
    llm, cloud = Mock(), Mock()
    monkeypatch.setattr("src.rainfall.executor.get_provider", llm)
    monkeypatch.setattr("src.rainfall.executor.bigquery.Client", cloud)
    report = execute(DEFAULT_BRD, DEFAULT_BRD.parent, tmp_path, dry_run=False)
    assert report["status"] == "BLOCKED"
    assert "gcloud auth application-default login" in report["issues"][0]["result"]
    llm.assert_not_called()
    cloud.assert_not_called()
    reports = list(tmp_path.rglob("run_report.md"))
    assert len(reports) == 1
    assert "| Requirement/check | Result | Action needed |" in reports[0].read_text()


def test_typed_metadata_and_repeated_schema_follow_contract(contract, rainfall_brd_available):
    column = contract.feeds[0].source_columns[0]
    assert typed_record({"station_id": "000123"}, [column]) == {"station_id": "000123"}
    assessment = next(
        table for table in extract_curated_contracts(read_document(rainfall_brd_available))
        if "assessment" in table.table_id
    )
    field = next(column for column in assessment.columns if column.name == "observation_run_dates")
    assert field_schema(field).field_type == "DATE"
    assert field_schema(field).mode == "REPEATED"


def test_authentication_guidance_distinguishes_login_from_iam_and_network():
    assert "gcloud auth application-default login" in authentication_guidance({"reason": "ADC_REFRESH_REJECTED"})
    assert "Re-login is not a permission fix" in authentication_guidance({"reason": "PERMISSION_DENIED"})
    assert "VPN/network" in authentication_guidance({"reason": "AUTH_TRANSPORT_ERROR"})


def test_idempotency_requires_zero_writes_not_just_equal_row_counts():
    client = Mock()
    child = Mock(statement_type="MERGE", job_id="child")
    client.list_jobs.return_value = [child]
    stats = client.get_job.return_value.dml_stats
    stats.inserted_row_count = 0
    stats.updated_row_count = 0
    stats.deleted_row_count = 0
    assert verify_unchanged_retry(client, "approved script", [], 1)["status"] == "PASS"
    stats.updated_row_count = 1
    with pytest.raises(ContractBlocked, match="idempotency verification failed"):
        verify_unchanged_retry(client, "approved script", [], 1)
