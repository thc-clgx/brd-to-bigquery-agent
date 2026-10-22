from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery
from pydantic import ValidationError

from src.brd_parser.document_reader import read_document
from src.orchestrator.managed_contract import ContractField, ManagedTable
from src.rainfall.executor import ContractBlocked, schema_actions
from src.rainfall.kpi import (
    capture_scope_sql, compile_kpi_sql, determine_scope, extract_kpi_contracts,
    semantic_fingerprint, SUPPORTED_RULES_SHA256,
)
from src.rainfall.preflight import approved_bucket, development_resources, extract_curated_contracts
from src.sql_generator.dsp import compile_dsp_publication
from src.sql_generator.render_template import SqlRenderer


def sample_table():
    return ManagedTable(
        table_id="sample-project-123.kpi_data.02_quality", layer="02",
        columns=[
            ContractField(name="station_id", type="STRING", nullable=False,
                          description="Station key", evidence_quote="Station key is required"),
            ContractField(name="month_start_date", type="DATE", nullable=False,
                          description="Calendar month", evidence_quote="Calendar month is required"),
        ], business_keys=["station_id", "month_start_date"], partition_by="month_start_date",
        partition_granularity="MONTH", cluster_by=["station_id"],
        dependencies=["sample-project-123.curated.01_readings"],
        select_sql="SELECT station_id, DATE_TRUNC(observation_date, MONTH) month_start_date "
                   "FROM `sample-project-123.curated.01_readings`",
        evidence_quote="One station per calendar month.",
    )


def test_dsp_adaptation_validates_before_atomic_mutations_and_never_fabricates_pubsub():
    table = sample_table()
    scope = "t.`month_start_date` IN UNNEST(@affected_months)"
    sql = compile_dsp_publication([table], scopes={table.table_id: scope})
    assert sql.index("CREATE TEMP TABLE") < sql.index("kpi_validation_failures") < sql.index("BEGIN TRANSACTION")
    assert sql.index("IF NOT EXISTS") < sql.index("BEGIN TRANSACTION") < sql.index("MERGE") < sql.index("COMMIT")
    assert sql.count("BEGIN TRANSACTION") == sql.count("COMMIT TRANSACTION") == 1
    assert "WHEN NOT MATCHED BY SOURCE AND " + scope + " THEN DELETE" in sql
    assert "TRUNCATE TABLE" not in sql and "SAFE_CAST" not in sql
    assert "ML_PIPELINE_RUN_UID" not in sql and "ampd_System_Logs" not in sql
    assert "ADAPTATION" in sql and "DSP 3.2.0" in sql


def test_generic_dsp_never_implies_scope_deletion_without_explicit_scope():
    sql = compile_dsp_publication([sample_table()])
    assert "WHEN NOT MATCHED BY SOURCE" not in sql
    assert "required_fields" in sql and "unique_key" in sql
    assert "SELECT table_name, business_key, rule, observed_value" in sql


def test_monthly_ddl_and_existing_granularity_mismatch():
    sql = SqlRenderer().render("managed_table.sql.j2", {
        "brd_sha256": "a" * 64, "table_id": sample_table().table_id,
        "columns": [column.model_dump() for column in sample_table().columns],
        "partition_by": "month_start_date", "partition_granularity": "MONTH", "cluster_by": ["station_id"],
    })
    assert "DATE_TRUNC(`month_start_date`, MONTH)" in sql
    actual = bigquery.Table(sample_table().table_id)
    wanted = bigquery.Table(sample_table().table_id)
    actual._properties["type"] = "TABLE"
    actual.time_partitioning = bigquery.TimePartitioning(field="month_start_date", type_="DAY")
    wanted.time_partitioning = bigquery.TimePartitioning(field="month_start_date", type_="MONTH")
    with pytest.raises(ContractBlocked, match="granularity"):
        schema_actions(actual, wanted, allow_nullable_additions=False)


def test_kpi_contract_rejects_missing_partition_and_unknown_layout():
    values = sample_table().model_dump()
    with pytest.raises(ValidationError, match="documented date partition"):
        ManagedTable(**{**values, "partition_by": None})
    with pytest.raises(ValidationError):
        ManagedTable(**{**values, "partition_granularity": "HOUR"})


def test_bucket_override_requires_project_original_and_destination_bound_approval(monkeypatch):
    resources = {"Environment": "dev", "Google Cloud project ID": "sample-project-123",
                 "GCS landing bucket": "original-private-dev"}
    monkeypatch.setenv("GCS_LANDING_BUCKET_OVERRIDE", "approved-private-dev")
    monkeypatch.delenv("GCS_BUCKET_OVERRIDE_APPROVAL", raising=False)
    with pytest.raises(ValueError, match="explicit DEV"):
        approved_bucket(resources)
    monkeypatch.setenv("GCS_BUCKET_OVERRIDE_APPROVAL",
                       "APPROVE_DEV:sample-project-123:original-private-dev:approved-private-dev")
    bucket, decision = approved_bucket(resources)
    assert bucket == "approved-private-dev"
    assert decision["brd_bucket"] == "original-private-dev"
    assert resources["GCS landing bucket"] == "original-private-dev"


def test_affected_scope_retains_failed_run_receipts_extends_next_day_and_crosses_month():
    client = MagicMock()
    client.query.return_value.result.return_value = [
        {"assessment_date": date(2026, 10, 31), "run_id": "failed-previous-run"},
    ]
    client.get_table.return_value = SimpleNamespace(num_rows=5)
    scope = determine_scope(client, [sample_table()], {
        "Google Cloud project ID": "sample-project-123", "02 dataset": "kpi_data",
    })
    assert scope["dates"] == [date(2026, 10, 31), date(2026, 11, 1)]
    assert scope["months"] == [date(2026, 10, 1), date(2026, 11, 1)]
    assert scope["pending"][0]["run_id"] == "failed-previous-run"
    assert not scope["bootstrap"]


def test_unchanged_inputs_do_not_expand_to_unaffected_history():
    client = MagicMock()
    client.query.return_value.result.return_value = []
    client.get_table.return_value = SimpleNamespace(num_rows=5)
    scope = determine_scope(client, [sample_table()], {
        "Google Cloud project ID": "sample-project-123", "02 dataset": "kpi_data",
    })
    assert scope["dates"] == scope["months"] == []
    assert client.query.call_count == 1


def test_empty_proximity_is_a_valid_result_not_a_reason_for_full_history_refresh():
    client = MagicMock()
    client.query.return_value.result.return_value = []
    client.get_table.side_effect = lambda identifier: SimpleNamespace(
        num_rows=0 if identifier.endswith("02_proximity") else 5,
    )
    proximity = sample_table().model_copy(update={"table_id": "sample-project-123.kpi_data.02_proximity"})
    scope = determine_scope(client, [sample_table(), proximity], {
        "Google Cloud project ID": "sample-project-123", "02 dataset": "kpi_data",
    })
    assert not scope["bootstrap"] and not scope["dates"]


def test_bootstrap_scope_is_extended_and_deduplicated():
    client = MagicMock()
    client.get_table.return_value = SimpleNamespace(num_rows=0)
    client.query.return_value.result.side_effect = [
        [], [{"assessment_date": date(2026, 10, 1)}, {"assessment_date": date(2026, 10, 3)}],
    ]
    scope = determine_scope(client, [sample_table()], {
        "Google Cloud project ID": "sample-project-123", "01 dataset": "curated", "02 dataset": "kpi_data",
    })
    assert scope["bootstrap"]
    assert scope["dates"] == [date(2026, 10, day) for day in range(1, 5)]


@pytest.fixture
def new_brd():
    path = Path(__file__).resolve().parents[2] / "BRD_hackathon_files" / "Regional_Rainfall_Flood_Watch_Project_BRD 2.docx"
    if not path.is_file():
        pytest.skip("Private hackathon BRD is not distributed in Git.")
    return read_document(path)


def test_new_brd_preserves_four_01_contracts_and_extracts_exact_four_kpi_schemas(new_brd):
    resources = development_resources(new_brd)
    assert len(extract_curated_contracts(new_brd)) == 4
    tables = extract_kpi_contracts(new_brd, resources)
    assert [len(table.columns) for table in tables] == [29, 23, 19, 22]
    assert [table.partition_granularity for table in tables] == ["DAY", "MONTH", "DAY", "MONTH"]
    assert all(not any(".00_" in dependency for dependency in table.dependencies) for table in tables)
    assert semantic_fingerprint(new_brd, resources) == SUPPORTED_RULES_SHA256
    assert [table.table_id for table in tables[:-1]][0] in tables[-1].dependencies


def test_rule_fingerprint_blocks_reusing_old_sql_for_changed_business_formulas(new_brd):
    for line in new_brd.lines:
        if "GOOD at 95.00" in line.text:
            line.text = line.text.replace("GOOD at 95.00", "GOOD at 90.00")
    with pytest.raises(ValueError, match="formulas/schema/layout differ"):
        extract_kpi_contracts(new_brd, development_resources(new_brd))


def test_new_kpi_sql_has_all_reconciliations_and_scope_ack_inside_transaction(new_brd):
    resources = development_resources(new_brd)
    tables = extract_kpi_contracts(new_brd, resources)
    sql = compile_kpi_sql(tables, resources)
    assert sql.count("MERGE `") == 4
    assert sql.count("WHEN NOT MATCHED BY SOURCE AND") == 4
    assert "coverage_rows_times_three" in sql
    assert "all_assessment_rows_accounted" in sql and "all_daily_rows_accounted" in sql
    assert "unmatched_expected_day" in sql and "eligible_assessment_count" in sql
    assert sql.index("consecutive_rank") < sql.index("BEGIN TRANSACTION")
    assert sql.index("BEGIN TRANSACTION") < sql.index("DELETE FROM `") < sql.index("COMMIT TRANSACTION")
    assert "FROM build_02_watershed_daily_rainfall_status" in sql
    assert sql.count("INSERT INTO kpi_validation_failures") == 1


def test_changed_01_dates_and_receipts_are_committed_with_01_publication(new_brd):
    initialization, publication = capture_scope_sql(
        extract_curated_contracts(new_brd), "sample-project-123.kpi_data._pipeline_02_pending_dates", "run123",
    )
    assert "FULL JOIN build_daily" in initialization
    assert "FULL JOIN build_assessment" in initialization
    assert "FULL JOIN build_coverage" in initialization
    assert "build_dimension" in initialization
    assert "transformed_at" not in initialization and "transform_run_date" not in initialization
    assert "WHERE NOT EXISTS" in publication and "'run123'" in publication
    assert "WHERE @bootstrap_02" in initialization
