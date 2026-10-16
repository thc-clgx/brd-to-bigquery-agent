from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from src.brd_parser.document_reader import read_document
from src.rainfall.preflight import (
    CuratedColumn,
    CuratedTable,
    development_resources,
    extract_curated_contracts,
    run_preflight,
    verify_workload_identity,
)
from src.rainfall.processing import _has_table_layout, _proposed_raw_dataset
from src.rainfall.runner import DEFAULT_BRD


def test_updated_brd_declares_seven_managed_table_contracts(rainfall_brd_available):
    document = read_document(rainfall_brd_available)
    resources = development_resources(document)
    assert resources["00 dataset"] == "rainfall_00_monitoring"
    assert resources["01 dataset"] == "rainfall_01_monitoring"
    assert resources["GCS landing bucket"] == "regional-rainfall-watch-dev"
    assert "Pipeline service account" not in resources
    tables = extract_curated_contracts(document)
    assert {table.table_id.rsplit(".", 1)[-1]: len(table.columns) for table in tables} == {
        "01_dim_station_watershed": 14,
        "01_fact_daily_station_rainfall": 15,
        "01_fact_station_rainfall_assessment": 28,
        "01_fact_watershed_station_coverage": 15,
    }
    assessment = next(table for table in tables if "assessment" in table.table_id)
    columns = {column.name: column for column in assessment.columns}
    assert columns["observation_source_files"].type == "ARRAY<STRING>"
    assert columns["observation_run_dates"].type == "ARRAY<DATE>"
    assert columns["observation_ingested_at_values"].type == "ARRAY<TIMESTAMP>"
    assert columns["window_complete"].type == "BOOL"
    assert not columns["window_complete"].nullable
    assert columns["rolling_rainfall_mm"].nullable


@pytest.mark.parametrize("value", ["FLOAT", "ARRAY<SECRET>", "STRING; DROP TABLE t", "{{type}}"])
def test_curated_contract_rejects_malformed_types(value):
    with pytest.raises(ValidationError):
        CuratedColumn(name="x", type=value, nullable=False, definition="test")


def test_curated_contract_rejects_unknown_fields_duplicate_columns_and_bad_identifier():
    column = {"name": "x", "type": "STRING", "nullable": False, "definition": "test"}
    with pytest.raises(ValidationError):
        CuratedColumn(**column, extra="ignored")
    for table_id, columns in (
        ("project.dataset.01_test", [column, column]),
        ("project.dataset.01_test; DROP TABLE x", [column]),
        ("project.dataset.01_test", []),
    ):
        with pytest.raises(ValidationError):
            CuratedTable(table_id=table_id, columns=columns)


def test_qualified_layout_and_legacy_layout_are_supported():
    table = "00_watershed_station_map"
    assert _has_table_layout(
        [[["project.dataset." + table, "effective_start_date", "watershed_id, station_id"]]],
        table, "effective_start_date", ["watershed_id", "station_id"],
    )
    assert _has_table_layout(
        [[["feed", table, "effective_start_date", "watershed_id, station_id"]]],
        table, "effective_start_date", ["watershed_id", "station_id"],
    )
    assert not _has_table_layout(
        [[["project.dataset." + table, "effective_end_date", "watershed_id, station_id"]]],
        table, "effective_start_date", ["watershed_id", "station_id"],
    )


def test_dataset_extraction_rejects_contradictory_targets():
    assert _proposed_raw_dataset(
        "project.rainfall_00_monitoring.00_flood_watch_thresholds"
    ) == "rainfall_00_monitoring"
    with pytest.raises(ValueError, match="contradictory"):
        _proposed_raw_dataset(
            "project.first_dataset.00_flood_watch_thresholds "
            "project.second_dataset.00_watershed_station_map"
        )


def test_curated_extraction_blocks_missing_schemas_and_nullability(rainfall_brd_available):
    document = read_document(rainfall_brd_available)
    missing = deepcopy(document)
    missing.tables = [
        table for table in missing.tables
        if not table or table[0][:3] != ["Column", "BigQuery type", "Nullable"]
    ]
    with pytest.raises(ValueError, match="all four"):
        extract_curated_contracts(missing)
    malformed = deepcopy(document)
    schema = next(
        table for table in malformed.tables
        if table and table[0][:3] == ["Column", "BigQuery type", "Nullable"]
    )
    schema[1][2] = "Maybe"
    with pytest.raises(ValueError, match="nullability"):
        extract_curated_contracts(malformed)


def test_preflight_persists_blocked_report_without_cloud_or_llm(
    tmp_path, rainfall_sources_available,
):
    report = run_preflight(
        rainfall_sources_available / DEFAULT_BRD.name, rainfall_sources_available,
        tmp_path, call_llm=False, check_identity=False,
    )
    assert report["status"] == "BLOCKED"
    assert report["cloud_mutations"] == []
    assert report["executed_sql"] == []
    assert report["llm"]["status"] == "NOT_CALLED"
    assert len(report["raw_contract"]["feeds"]) == 3
    assert len(report["curated_contracts"]) == 4
    assert (tmp_path / "reports" / f"strict_preflight_{report['run_id']}" / "run_report.md").exists()
    assert sum(item["accepted_rows"] for item in report["source_inventory"]) == 783


def test_optional_service_account_is_validated_when_declared(rainfall_brd_available):
    document = read_document(rainfall_brd_available)
    resource_table = next(
        table for table in document.tables
        if table and table[0][:2] == ["Resource", "Development value"]
    )
    resource_table.append([
        "Pipeline service account",
        "rainfall-pipeline@clgx-surveillance-dev-9e5e.iam.gserviceaccount.com",
        "Explicit workload identity",
    ])
    assert "Pipeline service account" in development_resources(document)
    resource_table[-1][1] = "invalid-identity"
    with pytest.raises(ValueError, match="workload identity"):
        development_resources(document)


def test_development_resources_accepts_a_brd_project_without_tenant_pinning(rainfall_brd_available):
    document = read_document(rainfall_brd_available)
    resource_table = next(
        table for table in document.tables
        if table and table[0][:2] == ["Resource", "Development value"]
    )
    resource = next(row for row in resource_table[1:] if row[0] == "Google Cloud project ID")
    resource[1] = "another-approved-project-123"
    assert development_resources(document)["Google Cloud project ID"] == resource[1]


def test_identity_uses_adc_when_no_service_account_is_required(monkeypatch):
    from unittest.mock import Mock

    source = Mock()
    impersonation = Mock()
    monkeypatch.setattr("google.auth.default", lambda **kwargs: (source, None))
    monkeypatch.setattr("google.auth.impersonated_credentials.Credentials", impersonation)
    result = verify_workload_identity()
    assert result == {"status": "PASS", "reason": "ADC_REFRESH_VERIFIED"}
    source.refresh.assert_called_once()
    impersonation.assert_not_called()


def test_required_impersonation_never_falls_back_to_adc(monkeypatch):
    from unittest.mock import Mock
    from google.auth.exceptions import RefreshError

    source = Mock()
    credentials = Mock()
    credentials.refresh.side_effect = RefreshError("NOT_FOUND sensitive upstream body")
    monkeypatch.setattr("google.auth.default", lambda **kwargs: (source, None))
    monkeypatch.setattr(
        "google.auth.impersonated_credentials.Credentials", Mock(return_value=credentials),
    )
    result = verify_workload_identity("explicit@project.iam.gserviceaccount.com")
    assert result == {"status": "BLOCKED", "reason": "NOT_FOUND"}
    source.refresh.assert_not_called()
