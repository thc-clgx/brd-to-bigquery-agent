from __future__ import annotations

import csv
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound

from src.models.pipeline_spec import ColumnSpec
from src.orchestrator.automatic_runner import environment_config, prepare_sources, provision, validate_value


def test_automatic_config_requires_project_and_does_not_reuse_legacy_defaults():
    with pytest.raises(ValueError, match="Configure"):
        environment_config({})
    with pytest.raises(ValueError, match="valid project identifier"):
        environment_config({"GOOGLE_CLOUD_PROJECT": "bad"})
    config = environment_config({
        "GOOGLE_CLOUD_PROJECT": "example-dev-project-123",
        "GOOGLE_CLOUD_LOCATION": "US",
        "GCS_LANDING_BUCKET": "test-bucket",
        "BQ_DATASET_00": "test_00",
        "LLM_PROVIDER": "litellm",
        "LLM_MODEL": "openai/test-model",
    })
    assert config.bigquery.dataset_01 is None
    assert config.gcs.overwrite_policy == "fail"
    assert config.gcp.project_id == "example-dev-project-123"


def test_automatic_csv_matching_is_by_headers_and_preserves_source(spec, tmp_path):
    source = tmp_path / "inputs"
    source.mkdir()
    path = source / "arbitrary_filename.csv"
    columns = spec.tables[0].file_columns
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([c.name for c in reversed(columns)])
        writer.writerow(["1" if c.type != "STRING" else "02013" for c in reversed(columns)])
    original = path.read_bytes()
    files = prepare_sources(spec, source, tmp_path / "staging")
    staged = files[spec.tables[0].source_name][0]
    with staged.open(newline="") as handle:
        records = list(csv.DictReader(handle))
    assert records[0]["countyFIPSCode"] == "02013"
    assert records[0]["Source_FileName"] == path.name
    assert path.read_bytes() == original


@pytest.mark.parametrize("datatype,text", [("INT64", "1.2"), ("FLOAT64", "NaN"), ("DATE", "2026-99-99"), ("BOOL", "yes")])
def test_automatic_source_type_checks_reject_invalid_values(datatype, text):
    with pytest.raises(ValueError, match="record rejected"):
        validate_value(text, ColumnSpec(name="value", type=datatype))


def test_automatic_cloud_pipeline_uploads_then_creates_both_tables_without_prompt(spec, config, tmp_path, monkeypatch):
    client = MagicMock()
    client.get_dataset.side_effect = NotFound("missing")
    client.get_table.side_effect = [NotFound("missing"), NotFound("missing"), MagicMock(), MagicMock()]
    storage_client = MagicMock()
    bucket = storage_client.get_bucket.return_value
    bucket.location = "US"
    bucket.blob.return_value.exists.return_value = False
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda **kwargs: client)
    monkeypatch.setattr("src.orchestrator.automatic_runner.GcsUploader",
                        lambda *args, **kwargs: SimpleNamespace(client=lambda: storage_client))
    table = spec.tables[0]
    source = tmp_path / "input.csv"
    source.write_text("header\nvalue\n")
    audit = provision(spec, config, {table.source_name: [source]}, {
        table.source_name: {"00": "CREATE EXTERNAL TABLE IF NOT EXISTS", "01": "CREATE TABLE IF NOT EXISTS"}
    })
    assert bucket.blob.return_value.upload_from_filename.call_args.kwargs["if_generation_match"] == 0
    assert client.query.call_count == 2
    assert [entry["action"] for entry in audit][-2:] == ["create_00", "create_01"]


def test_automatic_provisioning_blocks_existing_tables_before_upload(spec, config, tmp_path, monkeypatch):
    client = MagicMock()
    client.get_dataset.return_value.location = "US"
    bucket = MagicMock()
    bucket.location = "US"
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda **kwargs: client)
    monkeypatch.setattr("src.orchestrator.automatic_runner.GcsUploader",
                        lambda *args, **kwargs: SimpleNamespace(client=lambda: SimpleNamespace(get_bucket=lambda name: bucket)))
    with pytest.raises(ValueError, match="existing table"):
        provision(spec, config, {spec.tables[0].source_name: [tmp_path / "input.csv"]}, {})
    bucket.blob.assert_not_called()
    client.query.assert_not_called()
