"""Strict Pydantic contracts for the rainfall ingestion workflow."""

from __future__ import annotations

import re
from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

BQType = Literal["STRING", "DATE", "FLOAT64", "INT64", "TIMESTAMP"]
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TABLE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]{1,1024}$")


class RequirementEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requirement: str
    brd_section: str
    confidence: float = Field(ge=0.0, le=1.0)
    deterministic_validation: Literal["PASS", "WARN", "FAIL"]
    human_review_required: bool = False


class SourceColumn(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str
    type: BQType
    nullable: bool
    required_indicator: Literal["Yes", "No", "Conditional"]
    description: str
    evidence: RequirementEvidence

    @field_validator("name")
    @classmethod
    def valid_identifier(cls, value: str) -> str:
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError(f"Invalid BigQuery column identifier: {value!r}")
        return value

    @model_validator(mode="after")
    def consistent_nullability(self) -> "SourceColumn":
        if self.required_indicator == "Yes" and self.nullable:
            raise ValueError(f"Required field {self.name} cannot be nullable.")
        if self.required_indicator in {"No", "Conditional"} and not self.nullable:
            raise ValueError(f"{self.required_indicator} field {self.name} must be nullable.")
        return self


class FeedContract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    feed_id: Literal["rain_gauge_daily_observations", "watershed_station_map", "flood_watch_thresholds"]
    source_name: str
    table_00: str
    suggested_gcs_prefix: str
    source_columns: list[SourceColumn]
    business_key: list[str]
    partition_field: str
    cluster_fields: list[str]
    brd_section: str
    validation_rules: list[RequirementEvidence]

    @field_validator("table_00")
    @classmethod
    def valid_table_identifier(cls, value: str) -> str:
        if not _TABLE_IDENTIFIER.fullmatch(value):
            raise ValueError(f"Invalid BigQuery table identifier: {value!r}")
        if not value.startswith("00_"):
            raise ValueError(f"Raw table name must start with '00_': {value!r}")
        return value

    @model_validator(mode="after")
    def validate_schema_references(self) -> "FeedContract":
        names = [column.name for column in self.source_columns]
        columns = {column.name: column for column in self.source_columns}
        if not names:
            raise ValueError(f"{self.feed_id} has no source columns.")
        if len(names) != len(set(names)):
            raise ValueError(f"{self.feed_id} has duplicate source columns.")
        if not set(self.business_key) <= set(names):
            raise ValueError(f"{self.feed_id} business key references an unknown source column.")
        if self.partition_field not in names:
            raise ValueError(f"{self.feed_id} partition field is not in the source schema.")
        if any(field not in names for field in self.cluster_fields):
            raise ValueError(f"{self.feed_id} clustering references an unknown source column.")
        if len(self.cluster_fields) != len(set(self.cluster_fields)) or len(self.cluster_fields) > 4:
            raise ValueError(f"{self.feed_id} clustering must contain at most four distinct fields.")
        if self.partition_field not in {"observation_date", "effective_start_date", "valid_from"}:
            raise ValueError(f"{self.feed_id} has an unsupported partition field.")
        if columns[self.partition_field].type != "DATE":
            raise ValueError(f"{self.feed_id} partition field must have a supported DATE type.")
        supported_cluster_types = {
            "STRING", "INT64", "NUMERIC", "BIGNUMERIC", "BOOL",
            "DATE", "DATETIME", "TIMESTAMP", "GEOGRAPHY",
        }
        if any(columns[field].type not in supported_cluster_types for field in self.cluster_fields):
            raise ValueError(f"{self.feed_id} contains an unsupported clustering field type.")
        return self


class MetadataColumn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Literal["source_file_name", "ingested_at", "run_date"]
    type: BQType
    nullable: Literal[False] = False
    description: str
    evidence: RequirementEvidence

    @model_validator(mode="after")
    def validate_type(self) -> "MetadataColumn":
        expected_types = {
            "source_file_name": "STRING",
            "ingested_at": "TIMESTAMP",
            "run_date": "DATE",
        }
        if self.type != expected_types[self.name]:
            raise ValueError(f"Metadata field {self.name} must have type {expected_types[self.name]}.")
        return self


class RainfallPipelineSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spec_version: Literal["1.0"] = "1.0"
    environment: str = "dev"
    brd_file: str
    brd_sha256: str
    dataset_00: str
    dataset_00_is_approved: bool = False
    project_id: str | None = None
    location: str | None = None
    gcs_bucket: str | None = None
    run_date: date
    run_id: str
    idempotency_key: str
    ingested_at_utc: str
    metadata_columns: list[MetadataColumn]
    feeds: list[FeedContract]
    parser: Literal["deterministic_brd_tables_v1"] = "deterministic_brd_tables_v1"
    llm_provider: str | None = None
    llm_model: str | None = None
    llm_credential_configured: bool = False

    @model_validator(mode="after")
    def validate_pipeline(self) -> "RainfallPipelineSpec":
        expected_feeds = {
            "rain_gauge_daily_observations",
            "watershed_station_map",
            "flood_watch_thresholds",
        }
        actual = {feed.feed_id for feed in self.feeds}
        if actual != expected_feeds or len(self.feeds) != len(expected_feeds):
            raise ValueError(f"Expected exactly the three BRD feeds; found {sorted(actual)}.")
        if [column.name for column in self.metadata_columns] != [
            "source_file_name", "ingested_at", "run_date"
        ]:
            raise ValueError("The required three metadata columns must appear once, in BRD order.")
        if any(
            column.name in {meta.name for meta in self.metadata_columns}
            for feed in self.feeds
            for column in feed.source_columns
        ):
            raise ValueError("Pipeline metadata must not be part of a source CSV contract.")
        if not re.fullmatch(r"[a-f0-9]{64}", self.brd_sha256):
            raise ValueError("brd_sha256 must be a SHA-256 hex digest.")
        if not re.fullmatch(r"[a-f0-9]{64}", self.idempotency_key):
            raise ValueError("idempotency_key must be a SHA-256 hex digest.")
        if not _TABLE_IDENTIFIER.fullmatch(self.dataset_00):
            raise ValueError(f"Invalid BigQuery dataset identifier: {self.dataset_00!r}")
        if self.project_id is not None and not re.fullmatch(
            r"(?=.{6,30}$)[a-z][a-z0-9-]*[a-z0-9]", self.project_id
        ):
            raise ValueError("project_id is not a valid Google Cloud project identifier.")
        if self.location is not None and not re.fullmatch(r"[A-Za-z0-9-]+", self.location):
            raise ValueError("location contains invalid characters.")
        if self.gcs_bucket is not None and not re.fullmatch(
            r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]", self.gcs_bucket
        ):
            raise ValueError("gcs_bucket is not a valid Cloud Storage bucket name.")
        return self


class RainfallLLMFinding(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    category: Literal[
        "source_feed_mapping",
        "feed_relationship",
        "conditional_validation",
        "proposed_01_requirement",
        "unresolved_decision",
    ]
    feed_id: Literal[
        "rain_gauge_daily_observations",
        "watershed_station_map",
        "flood_watch_thresholds",
    ] | None = None
    source_file: str | None = None
    field_names: list[str] = Field(default_factory=list, max_length=20)
    statement: str = Field(min_length=1, max_length=2000)
    brd_section: str = Field(min_length=1, max_length=200)
    evidence_quote: str = Field(min_length=1, max_length=2000)
    confidence: float = Field(ge=0.0, le=1.0)
    human_review_required: bool

    @model_validator(mode="after")
    def require_human_review(self) -> "RainfallLLMFinding":
        if not self.human_review_required:
            raise ValueError("LLM findings are advisory and must require human review.")
        return self


class RainfallLLMAnalysis(BaseModel):
    model_config = ConfigDict(extra="forbid")

    summary: str = Field(min_length=1, max_length=4000)
    findings: list[RainfallLLMFinding] = Field(default_factory=list, max_length=40)
