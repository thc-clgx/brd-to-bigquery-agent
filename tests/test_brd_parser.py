"""End-to-end rule-based parsing of a Markdown BRD."""

from __future__ import annotations

from datetime import date

import pytest

from src.brd_parser.document_reader import read_document
from src.brd_parser.requirement_extractor import BrdParseError, RequirementExtractor
from src.brd_parser.spec_validator import validate_spec

BRD = """\
# Wildfire Risk Score

## Input Data
  Four Data Sources
    Brushfire (vegetation)
      Change Log Location
        gs://geospatial-data-warehouse/risk/Brushfire/Brushfire_<YYYY>_<MM>/Support/Brushfire_Change_Logs/<stateAbbrev>_Change_Log.xlsx
      Fields
        countyFIPSCode STRING
          Ensure leading zero retained
        riskValue INTEGER
        previousAcres FLOAT
        newAcres FLOAT
        acreageChange FLOAT
        previousCountyPct FLOAT
        newCountyPct FLOAT
        netCountyPctChg FLOAT
    Drought
      Change Log Location
        gs://geospatial-data-warehouse/risk/WildfireDrought/WildfireDrought_<YYYY>_<MM>/Support/WildfireDrought_Change_Log.xlsx
      Fields
        stateCode STRING
          Ensure leading zero retained
        exPoints INTEGER
        previousAcres FLOAT
        newAcres FLOAT
        acreageChange FLOAT
        previousStatePct FLOAT
        newStatePct FLOAT
        netStatePctChg FLOAT
  Pipeline
    Standard Core pipeline (GCS->00->01) for each input
    Convert .xlsx to .csv, then upload to amp'd GCS bucket
    For brush & firebreak, files are state level, so 00 table should use wildcard (e.g., *.csv).
    01 table to include multiple runs
      Add run identifier
      Add LOAD_DATE_01
      Partition by run_date
      Cluster by countyFIPSCode or stateCode, as appropriate based on table contents
    CRITERIA_PROCEDURE
      Allow for re-run, replacing entire run
  Development Location:
    Project: clgx-surveillance-dev-9e5e
    GCS
      Bucket: clgx-surveillance-dev-hazard
      Folder: 00_wfrs_brushfire_changelogs
      Folder: 00_wfrs_drought_changelogs
    Data Sets:
      00: ampd_00_hazard_wildfire
      01: ampd_01_hazard_wildfire
    Table Names:
      00_wfrs_brushfire_changelogs
      00_wfrs_drought_changelogs
      01_wfrs_brushfire_changelogs_county
      01_wfrs_drought_changelogs_state
    Schedule: Annual upon copy of files to amp'd GCS bucket
    Historical Load:
      2025_04
      2026_04
"""


@pytest.fixture
def parsed_spec(tmp_path, config):
    path = tmp_path / "brd.md"
    path.write_text(BRD, encoding="utf-8")
    document = read_document(path)
    return RequirementExtractor(config).extract(document, run_id="2026_04")


def test_header_values_are_extracted(parsed_spec):
    assert parsed_spec.project_id == "clgx-surveillance-dev-9e5e"
    assert parsed_spec.gcs_bucket == "clgx-surveillance-dev-hazard"
    assert parsed_spec.dataset_00 == "ampd_00_hazard_wildfire"
    assert parsed_spec.dataset_01 == "ampd_01_hazard_wildfire"
    assert parsed_spec.historical_runs == ["2025_04", "2026_04"]
    assert parsed_spec.run_date == date(2026, 4, 1)


def test_tables_are_paired_and_typed(parsed_spec):
    by_00 = {t.table_00: t for t in parsed_spec.tables}
    assert set(by_00) == {"00_wfrs_brushfire_changelogs", "00_wfrs_drought_changelogs"}

    brushfire = by_00["00_wfrs_brushfire_changelogs"]
    assert brushfire.table_01 == "01_wfrs_brushfire_changelogs_county"
    assert brushfire.grain == "county"
    assert brushfire.cluster_by == ["countyFIPSCode"]
    assert brushfire.filename_state_pattern == "stateAbbrev"
    assert brushfire.source_glob == "*_Change_Log.xlsx"

    drought = by_00["00_wfrs_drought_changelogs"]
    assert drought.table_01 == "01_wfrs_drought_changelogs_state"
    assert drought.grain == "state"
    assert drought.cluster_by == ["stateCode"]
    assert drought.filename_state_pattern is None


def test_float_is_normalized_and_metadata_is_appended(parsed_spec):
    brushfire = parsed_spec.table("Brushfire (vegetation)")
    types = {c.name: c.type for c in brushfire.schema_columns}
    assert types["previousAcres"] == "FLOAT64"
    assert types["countyFIPSCode"] == "STRING"
    assert types["run_date"] == "DATE"
    assert types["Source_FileName"] == "STRING"
    assert types["LOAD_DATE_01"] == "DATE"
    assert [c.name for c in brushfire.schema_columns][-3:] == [
        "run_date", "Source_FileName", "LOAD_DATE_01",
    ]


def test_leading_zero_flag_is_captured(parsed_spec):
    brushfire = parsed_spec.table("Brushfire (vegetation)")
    column = next(c for c in brushfire.schema_columns if c.name == "countyFIPSCode")
    assert column.preserve_leading_zeros


def test_parsed_spec_passes_validation(parsed_spec, config):
    report = validate_spec(parsed_spec, config)
    assert report.passed, [str(r) for r in report.failures]


def test_missing_deployment_block_raises(tmp_path, config):
    path = tmp_path / "empty.md"
    path.write_text("# Nothing useful here\n", encoding="utf-8")
    with pytest.raises(BrdParseError):
        RequirementExtractor(config).extract(read_document(path), run_id="2026_04")


def test_text_pdf_reader_preserves_page_provenance(tmp_path):
    from pypdf import PdfWriter
    from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject

    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    font = DictionaryObject({
        NameObject("/Type"): NameObject("/Font"),
        NameObject("/Subtype"): NameObject("/Type1"),
        NameObject("/BaseFont"): NameObject("/Helvetica"),
    })
    page[NameObject("/Resources")] = DictionaryObject({
        NameObject("/Font"): DictionaryObject({
            NameObject("/F1"): writer._add_object(font),
        }),
    })
    stream = DecodedStreamObject()
    stream.set_data(b"BT /F1 12 Tf 72 720 Td (Customer daily observations) Tj ET")
    page[NameObject("/Contents")] = writer._add_object(stream)
    path = tmp_path / "brd.pdf"
    writer.write(path)

    document = read_document(path)
    assert "Customer daily observations" in document.text
    assert document.lines[0].source == "page:1"
    assert document.path == path


def test_pdf_reader_rejects_pages_without_extractable_text(tmp_path):
    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    path = tmp_path / "scanned.pdf"
    writer.write(path)
    with pytest.raises(ValueError, match="OCR-reviewed"):
        read_document(path)


def test_pdf_reader_rejects_encrypted_brd(tmp_path):
    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    writer.encrypt("unit-test-password")
    path = tmp_path / "encrypted.pdf"
    writer.write(path)
    with pytest.raises(ValueError, match="Encrypted BRDs"):
        read_document(path)
