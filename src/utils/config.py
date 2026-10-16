"""Loads and validates the YAML environment config (``configs/dev.yaml``)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class GcpConfig(BaseModel):
    # None means "take it from the BRD". Set it to pin/override the BRD value.
    project_id: str | None = None
    location: str = "US"
    impersonate_service_account: str | None = None


class GcsConfig(BaseModel):
    bucket: str | None = None
    overwrite_policy: str = Field(default="fail", pattern="^(skip|overwrite|fail)$")


class ExternalTableOptions(BaseModel):
    allow_jagged_rows: bool = False
    allow_quoted_newlines: bool = False
    format: str = "csv"
    skip_leading_rows: int = 1
    field_delimiter: str = ","
    quote: str = ""


class BigQueryConfig(BaseModel):
    dataset_00: str | None = None
    dataset_01: str | None = None
    external_all_strings: bool = True
    external_table_options: ExternalTableOptions = Field(default_factory=ExternalTableOptions)


class PipelineConfig(BaseModel):
    rerun_strategy: str = Field(default="replace_run", pattern="^(replace_run|append|truncate_table)$")
    partition_field: str = "run_date"
    run_identifier_field: str = "run_date"
    load_date_field: str = "LOAD_DATE_01"
    source_file_field: str = "Source_FileName"
    source_format: str = "xlsx"
    target_format: str = "csv"


class SafetyConfig(BaseModel):
    dry_run: bool = True
    approval_required: bool = True
    approval_token: str = "APPROVE_DEV_RUN"
    allow_destructive: bool = False
    forbidden_projects: list[str] = Field(default_factory=list)


class PathsConfig(BaseModel):
    templates_dir: str = "templates"
    generated_sql_dir: str = "generated/sql"
    generated_specs_dir: str = "generated/specs"
    generated_reports_dir: str = "generated/reports"
    staging_csv_dir: str = "generated/staging_csv"


class FilePrepConfig(BaseModel):
    leading_zero_columns: list[str] = Field(default_factory=list)
    max_header_scan_rows: int = 20
    csv_encoding: str = "utf-8"
    csv_quote_char: str = ""


class LoggingConfig(BaseModel):
    level: str = "INFO"


class LlmConfig(BaseModel):
    """LLM and agentic-decision settings.

    Disabled by default: the framework must run offline and deterministically
    unless someone opts in. No credentials live here - API keys are read from
    the environment by the provider.
    """

    enabled: bool = False
    # rule_based | llm | hybrid
    extraction_mode: str = "hybrid"
    # auto | copilot_cli | openai | anthropic | gemini | none | echo
    provider: str = "auto"
    model: str = ""
    timeout_seconds: int = 300
    # Agentic repair turns after the first proposal.
    max_attempts: int = 3
    # Below this, a decision is flagged for human review at the approval gate.
    min_confidence: float = 0.7
    max_document_chars: int = 60000
    # Use the model to resolve source headers the deterministic matcher missed.
    assist_header_mapping: bool = True
    # argv for copilot_cli; "{prompt}" is substituted.
    cli_command: list[str] = Field(default_factory=list)

    @field_validator("extraction_mode")
    @classmethod
    def _mode(cls, value: str) -> str:
        allowed = {"rule_based", "llm", "hybrid"}
        if value not in allowed:
            raise ValueError(f"extraction_mode must be one of {sorted(allowed)}, got {value!r}")
        return value

    @field_validator("min_confidence")
    @classmethod
    def _confidence(cls, value: float) -> float:
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"min_confidence must be between 0 and 1, got {value}")
        return value


class AppConfig(BaseModel):
    """Typed view over ``configs/<env>.yaml``."""

    model_config = ConfigDict(extra="forbid")

    environment: str
    gcp: GcpConfig
    gcs: GcsConfig
    bigquery: BigQueryConfig
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    safety: SafetyConfig = Field(default_factory=SafetyConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)
    file_prep: FilePrepConfig = Field(default_factory=FilePrepConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    llm: LlmConfig = Field(default_factory=LlmConfig)

    config_path: Path | None = None

    def resolve(self, relative: str) -> Path:
        """Resolve a config-relative path against the project root."""
        path = Path(relative)
        return path if path.is_absolute() else (PROJECT_ROOT / path)

    def assert_project_allowed(self) -> None:
        for forbidden in self.safety.forbidden_projects:
            if forbidden and self.gcp.project_id and forbidden in self.gcp.project_id:
                raise ValueError(
                    f"Refusing to run: project {self.gcp.project_id!r} matches forbidden pattern {forbidden!r}."
                )


def load_config(config_path: str | Path) -> AppConfig:
    path = Path(config_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    raw: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    config = AppConfig(**raw, config_path=path)
    config.assert_project_allowed()
    return config
