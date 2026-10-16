from datetime import date

import pytest

from src.brd_parser.spec_validator import validate_spec
from src.models.pipeline_spec import ColumnSpec, PipelineSpec, TableSpec


def test_valid_spec_passes(spec, config):
    report = validate_spec(spec, config)
    assert report.passed, [str(r) for r in report.failures]


def test_missing_run_date_column_fails(spec, config):
    table = spec.tables[0]
    table.schema_columns = [c for c in table.schema_columns if c.name != "run_date"]
    report = validate_spec(spec, config)
    assert not report.passed
    assert any("run_date" in r.name for r in report.failures)


def test_missing_source_filename_fails(spec, config):
    table = spec.tables[0]
    table.schema_columns = [c for c in table.schema_columns if c.name != "Source_FileName"]
    report = validate_spec(spec, config)
    assert any("has_source_file" in r.name for r in report.failures)


def test_county_code_must_be_string(spec, config):
    column = next(c for c in spec.tables[0].schema_columns if c.name == "countyFIPSCode")
    column.type = "INTEGER"
    column.preserve_leading_zeros = False
    report = validate_spec(spec, config)
    assert any("grain_column_is_string" in r.name for r in report.failures)


def test_cluster_field_must_exist_in_schema(spec, config):
    spec.tables[0].cluster_by = ["not_a_column"]
    report = validate_spec(spec, config)
    assert any("cluster_fields" in r.name for r in report.failures)


def test_same_dataset_for_00_and_01_fails(spec, config):
    spec.dataset_01 = spec.dataset_00
    report = validate_spec(spec, config)
    assert any("datasets_distinct" in r.name for r in report.failures)


def test_table_00_prefix_is_enforced_by_the_model():
    with pytest.raises(ValueError):
        TableSpec(
            source_name="bad",
            table_00="wfrs_bad",
            table_01="01_wfrs_bad",
            gcs_folder="x",
            file_pattern="x_",
            schema=[],
        )


def test_duplicate_columns_rejected():
    with pytest.raises(ValueError):
        TableSpec(
            source_name="dupe",
            table_00="00_x",
            table_01="01_x",
            gcs_folder="x",
            file_pattern="x_",
            schema=[ColumnSpec(name="a"), ColumnSpec(name="a")],
        )


def test_spec_round_trips_through_json(spec, tmp_path):
    path = spec.save(tmp_path)
    reloaded = PipelineSpec.load(path)
    assert reloaded.run_date == date(2026, 4, 1)
    assert reloaded.tables[0].table_01 == spec.tables[0].table_01
    assert [c.name for c in reloaded.tables[0].schema_columns] == [
        c.name for c in spec.tables[0].schema_columns
    ]


def test_float_alias_is_normalized():
    assert ColumnSpec(name="x", type="FLOAT").type == "FLOAT64"
