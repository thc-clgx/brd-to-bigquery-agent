"""Shared pytest fixtures."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.models.pipeline_spec import ColumnSpec, PipelineSpec, TableSpec  # noqa: E402
from src.utils.config import load_config  # noqa: E402
from src.rainfall.runner import DEFAULT_BRD  # noqa: E402


@pytest.fixture(scope="session")
def config():
    return load_config(PROJECT_ROOT / "configs" / "dev.yaml")


@pytest.fixture(scope="session")
def legacy_config():
    configured = load_config(PROJECT_ROOT / "configs" / "dev.yaml")
    configured.gcp.project_id = "brd-agent-test-123"
    configured.gcs.bucket = "brd-agent-test-bucket"
    return configured


@pytest.fixture
def rainfall_brd_available():
    if not DEFAULT_BRD.is_file():
        pytest.skip("The private rainfall BRD fixture is not included in Git.")
    return DEFAULT_BRD


@pytest.fixture
def rainfall_sources_available(rainfall_brd_available):
    expected_sources = {
        "regional_rainfall_daily_observations.csv",
        "regional_watershed_station_map.csv",
        "regional_flood_watch_thresholds.csv",
    }
    available_sources = {path.name for path in rainfall_brd_available.parent.glob("*.csv")}
    if not expected_sources <= available_sources:
        pytest.skip("Private rainfall source fixtures are not included in Git.")
    return rainfall_brd_available.parent


def _county_columns() -> list[ColumnSpec]:
    return [
        ColumnSpec(name="countyFIPSCode", type="STRING", preserve_leading_zeros=True),
        ColumnSpec(name="riskValue", type="INTEGER"),
        ColumnSpec(name="previousAcres", type="FLOAT64"),
        ColumnSpec(name="newAcres", type="FLOAT64"),
        ColumnSpec(name="acreageChange", type="FLOAT64"),
        ColumnSpec(name="previousCountyPct", type="FLOAT64"),
        ColumnSpec(name="newCountyPct", type="FLOAT64"),
        ColumnSpec(name="netCountyPctChg", type="FLOAT64"),
        ColumnSpec(name="run_date", type="DATE", is_metadata=True, nullable=False),
        ColumnSpec(name="Source_FileName", type="STRING", is_metadata=True),
        ColumnSpec(name="LOAD_DATE_01", type="DATE", is_metadata=True, added_at_load=True, nullable=False),
    ]


@pytest.fixture
def brushfire_table() -> TableSpec:
    return TableSpec(
        source_name="Brushfire (vegetation)",
        grain="county",
        table_00="00_wfrs_brushfire_changelogs",
        table_01="01_wfrs_brushfire_changelogs_county",
        gcs_folder="00_wfrs_brushfire_changelogs",
        file_pattern="wfrs_brushfire_changelogs_",
        schema=_county_columns(),
        partition_by="run_date",
        cluster_by=["countyFIPSCode"],
        filename_state_pattern="stateAbbrev",
        source_glob="*_Change_Log.xlsx",
    )


@pytest.fixture
def spec(brushfire_table: TableSpec) -> PipelineSpec:
    return PipelineSpec(
        environment="dev",
        project_id="clgx-surveillance-dev-9e5e",
        gcs_bucket="clgx-surveillance-dev-hazard",
        dataset_00="ampd_00_hazard_wildfire",
        dataset_01="ampd_01_hazard_wildfire",
        run_id="2026_04",
        run_date=date(2026, 4, 1),
        historical_runs=["2025_04", "2026_04"],
        tables=[brushfire_table],
        brd_source="tests/fixture.docx",
    )
