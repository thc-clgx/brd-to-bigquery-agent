"""Structured pipeline spec.

Everything downstream (SQL generation, file prep, GCS upload, BigQuery
execution, validation) reads from :class:`PipelineSpec`. The BRD parser is the
only component that knows about documents; swapping the rule-based parser for an
LLM parser later only needs to keep producing this same object.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

BigQueryType = Literal[
    "STRING", "BYTES", "INT64", "INTEGER", "FLOAT64", "NUMERIC",
    "BIGNUMERIC", "BOOL", "DATE", "DATETIME", "TIME", "TIMESTAMP",
]

Grain = Literal["county", "state", "zipcode", "property", "other"]

# BRD prose uses loose type names; BigQuery DDL needs canonical ones.
TYPE_ALIASES = {
    "FLOAT": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "DECIMAL": "NUMERIC",
    "INT": "INT64",
    "INTEGER": "INTEGER",
    "BOOLEAN": "BOOL",
    "VARCHAR": "STRING",
    "TEXT": "STRING",
    "CHAR": "STRING",
}


def normalize_bq_type(raw: str) -> str:
    token = (raw or "").strip().upper()
    return TYPE_ALIASES.get(token, token)


class ColumnSpec(BaseModel):
    """One column of the 01 table (and, as STRING, of the 00 external table)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    type: BigQueryType = "STRING"
    nullable: bool = True
    description: str | None = None
    # Header text in the source workbook, when it differs from ``name``.
    source_header: str | None = None
    preserve_leading_zeros: bool = False
    # True for run_date / Source_FileName / LOAD_DATE_01 - not present in the raw file.
    is_metadata: bool = False
    # LOAD_DATE_01 is stamped at load time and does not exist in the 00 table.
    added_at_load: bool = False

    @field_validator("type", mode="before")
    @classmethod
    def _normalize_type(cls, value: str) -> str:
        return normalize_bq_type(str(value))

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        token = value.strip()
        if not token:
            raise ValueError("Column name cannot be empty.")
        if not token.replace("_", "a").isalnum() or token[0].isdigit():
            raise ValueError(f"Invalid BigQuery column name: {value!r}")
        return token


class TableSpec(BaseModel):
    """A single GCS -> 00 -> 01 table lineage."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    source_name: str
    grain: Grain = "other"

    table_00: str
    table_01: str
    gcs_folder: str
    # Object-name prefix for the generated CSVs; the 00 URI appends ``*.csv``.
    file_pattern: str

    schema_columns: list[ColumnSpec] = Field(default_factory=list, alias="schema")

    partition_by: str = "run_date"
    cluster_by: list[str] = Field(default_factory=list)

    source_format: str = "xlsx"
    target_format: str = "csv"
    wildcard_required: bool = True
    rerun_strategy: str = "replace_run"

    # --- file preparation hints -------------------------------------------
    # Local glob of the raw workbooks for this source, relative to --source-root.
    source_glob: str | None = None
    # ``<stateAbbrev>_Change_Log.xlsx`` -> state abbreviation becomes a filename suffix.
    filename_state_pattern: str | None = None
    source_location_hint: str | None = None

    # --- provenance --------------------------------------------------------
    brd_section: str | None = None

    # --- physical source layout (set by file_prep.source_profiler) ---------
    # Real header order of the staged CSV. When set, the 00 external table
    # mirrors this instead of the BRD schema, and the load normalises back.
    source_headers: list[str] = Field(default_factory=list)
    # BRD column name -> physical header.
    source_map: dict[str, str] = Field(default_factory=dict)
    # BRD columns absent from the file; supplied as literals at load time.
    source_missing: list[str] = Field(default_factory=list)
    # Explicit 00 URI, set when profiling finds files that do not match file_pattern.
    source_uri: str | None = None

    @model_validator(mode="after")
    def _check(self) -> "TableSpec":
        if not self.table_00.startswith("00"):
            raise ValueError(f"table_00 must start with '00': {self.table_00!r}")
        if not self.table_01.startswith("01"):
            raise ValueError(f"table_01 must start with '01': {self.table_01!r}")
        names = [c.name for c in self.schema_columns]
        if len(names) != len(set(names)):
            dupes = sorted({n for n in names if names.count(n) > 1})
            raise ValueError(f"Duplicate columns in {self.source_name}: {dupes}")
        return self

    # --- derived views -----------------------------------------------------
    @property
    def columns_01(self) -> list[ColumnSpec]:
        """Full 01 schema, including LOAD_DATE_01."""
        return list(self.schema_columns)

    @property
    def columns_00(self) -> list[ColumnSpec]:
        """00 external-table columns: everything that physically exists in the CSV."""
        return [c for c in self.schema_columns if not c.added_at_load]

    @property
    def is_profiled(self) -> bool:
        return bool(self.source_headers)

    def physical_name(self, column_name: str) -> str | None:
        """Physical 00 column backing a BRD column, or None if the file lacks it."""
        if not self.is_profiled:
            return column_name
        return self.source_map.get(column_name)

    @property
    def csv_columns(self) -> list[str]:
        """Exact CSV header order produced by file preparation."""
        return [c.name for c in self.columns_00]

    @property
    def file_columns(self) -> list[ColumnSpec]:
        """Columns read from the raw workbook (metadata excluded)."""
        return [c for c in self.schema_columns if not c.is_metadata]

    @property
    def leading_zero_columns(self) -> list[str]:
        return [c.name for c in self.schema_columns if c.preserve_leading_zeros]

    def gcs_uri_wildcard(self, bucket: str) -> str:
        if self.source_uri:
            return self.source_uri
        suffix = "*" if self.wildcard_required else ""
        return f"gs://{bucket}/{self.gcs_folder}/{self.file_pattern}{suffix}.{self.target_format}"

    def gcs_prefix(self) -> str:
        return f"{self.gcs_folder}/"

    def csv_file_name(self, run_id: str, suffix: str | None = None) -> str:
        parts = [self.file_pattern.rstrip("_"), run_id]
        if suffix:
            parts.append(suffix)
        return "_".join(parts) + f".{self.target_format}"


class PipelineSpec(BaseModel):
    """Machine-readable contract extracted from a BRD."""

    model_config = ConfigDict(extra="forbid")

    spec_version: str = "1.0"
    environment: str = "dev"
    project_id: str
    location: str = "US"
    gcs_bucket: str
    dataset_00: str
    dataset_01: str

    run_id: str
    run_date: date

    pipeline_type: str = "GCS->00->01"
    rerun_strategy: str = "replace_run"
    schedule: str | None = None
    historical_runs: list[str] = Field(default_factory=list)

    tables: list[TableSpec] = Field(default_factory=list)

    brd_source: str | None = None
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    parser: str = "rule_based_v1"
    warnings: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> "PipelineSpec":
        names_00 = [t.table_00 for t in self.tables]
        names_01 = [t.table_01 for t in self.tables]
        for label, names in (("table_00", names_00), ("table_01", names_01)):
            if len(names) != len(set(names)):
                dupes = sorted({n for n in names if names.count(n) > 1})
                raise ValueError(f"Duplicate {label} entries in spec: {dupes}")
        return self

    def table(self, source_name: str) -> TableSpec:
        for table in self.tables:
            if table.source_name == source_name:
                return table
        raise KeyError(f"No table named {source_name!r} in spec.")

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(
            self.model_dump(mode="json", by_alias=True), indent=indent, sort_keys=False
        )

    def save(self, directory: str | Path) -> Path:
        out_dir = Path(directory)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"pipeline_spec_{self.environment}_{self.run_id}.json"
        path.write_text(self.to_json(), encoding="utf-8")
        return path

    @classmethod
    def load(cls, path: str | Path) -> "PipelineSpec":
        return cls.model_validate_json(Path(path).read_text(encoding="utf-8"))
