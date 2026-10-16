"""Leading-zero preservation and xlsx -> csv conversion."""

from __future__ import annotations

import pandas as pd
import pytest

from src.file_prep.xlsx_to_csv import XlsxToCsvConverter, normalize_header, validate_csv_outputs
from src.models.validation import ValidationReport

SOURCE_HEADERS = [
    "County FIPS Code",
    "Risk Value",
    "Previous Acres",
    "New Acres",
    "Acreage Change",
    "Previous County %",
    "New County %",
    "Net % Change in County",
]


@pytest.fixture
def workbook(tmp_path, brushfire_table):
    """A workbook shaped like the real change logs: three banner rows, then data."""
    path = tmp_path / "AK_Change_Log.xlsx"
    banner = pd.DataFrame([[None] * 8] * 3)
    data = pd.DataFrame(
        [
            ["02013", "1", "11219604.48", "11219604.48", "0", "97.67", "97.67", "0"],
            ["02016", "2", "100.5", "120.5", "20", "1.25", "1.5", "0.25"],
        ],
        columns=SOURCE_HEADERS,
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        banner.to_excel(writer, index=False, header=False, startrow=0)
        data.to_excel(writer, index=False, startrow=3)
    return path


def test_normalize_header_handles_prose_headers():
    assert normalize_header("County FIPS Code") == normalize_header("countyFIPSCode")
    assert normalize_header("Previous County %") == normalize_header("previousCountyPct")
    assert normalize_header("Acreage Change") == normalize_header("acreageChange")


def test_conversion_preserves_leading_zeros(workbook, tmp_path, spec, brushfire_table, config):
    converter = XlsxToCsvConverter(config, dry_run=False)
    result = converter.convert_file(spec, brushfire_table, workbook, tmp_path / "out")

    assert result.row_count == 2
    assert result.file_suffix == "AK"
    assert result.csv_path.name == "wfrs_brushfire_changelogs_2026_04_AK.csv"

    text = result.csv_path.read_text(encoding="utf-8")
    assert "02013" in text, text
    frame = pd.read_csv(result.csv_path, dtype=str, keep_default_na=False)
    assert frame["countyFIPSCode"].tolist() == ["02013", "02016"]


def test_csv_header_matches_the_00_schema(workbook, tmp_path, spec, brushfire_table, config):
    converter = XlsxToCsvConverter(config, dry_run=False)
    result = converter.convert_file(spec, brushfire_table, workbook, tmp_path / "out")
    assert result.columns == brushfire_table.csv_columns
    assert "LOAD_DATE_01" not in result.columns


def test_metadata_columns_are_added(workbook, tmp_path, spec, brushfire_table, config):
    converter = XlsxToCsvConverter(config, dry_run=False)
    result = converter.convert_file(spec, brushfire_table, workbook, tmp_path / "out")
    frame = pd.read_csv(result.csv_path, dtype=str, keep_default_na=False)
    assert frame["run_date"].unique().tolist() == ["2026-04-01"]
    assert frame["Source_FileName"].unique().tolist() == ["AK_Change_Log.xlsx"]


def test_dry_run_writes_nothing(workbook, tmp_path, spec, brushfire_table, config):
    converter = XlsxToCsvConverter(config, dry_run=True)
    result = converter.convert_file(spec, brushfire_table, workbook, tmp_path / "out")
    assert result.skipped
    assert not result.csv_path.exists()


def test_file_validations_pass(workbook, tmp_path, spec, brushfire_table, config):
    converter = XlsxToCsvConverter(config, dry_run=False)
    results = [converter.convert_file(spec, brushfire_table, workbook, tmp_path / "out")]
    report = ValidationReport()
    validate_csv_outputs(brushfire_table, results, report, dry_run=False)
    assert report.passed, [str(r) for r in report.failures]


def test_empty_file_is_flagged(tmp_path, spec, brushfire_table, config):
    path = tmp_path / "AK_Change_Log.xlsx"
    pd.DataFrame(columns=SOURCE_HEADERS).to_excel(path, index=False)

    converter = XlsxToCsvConverter(config, dry_run=False)
    results = [converter.convert_file(spec, brushfire_table, path, tmp_path / "out")]
    report = ValidationReport()
    validate_csv_outputs(brushfire_table, results, report, dry_run=False)
    assert any("row_count" in r.name for r in report.failures)
