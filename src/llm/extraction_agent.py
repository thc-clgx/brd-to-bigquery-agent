"""Agentic BRD extraction: propose -> validate -> critique -> repair.

This is the agentic part of the framework. The model does not get one shot at
the document. Its proposal is compiled into a ``PipelineSpec`` and run through
the same deterministic validator the rule-based path uses; every failure is fed
back as a critique and the model tries again, up to a bounded number of
attempts.

The loop only ever terminates in one of two states: a spec that passes every
deterministic check, or a failure. A model response never becomes a spec by
assertion alone.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError

from ..brd_parser.document_reader import BrdDocument
from ..brd_parser.spec_validator import validate_spec
from ..models.pipeline_spec import ColumnSpec, PipelineSpec, TableSpec, normalize_bq_type
from ..utils.config import AppConfig
from ..utils.logging_setup import get_logger
from ..utils.run_date import normalize_run_id, parse_run_date
from .provider import LLMProvider, LLMError, extract_json
from .prompts import SYSTEM_EXTRACTOR, extraction_prompt, repair_prompt

logger = get_logger(__name__)


@dataclass
class Attempt:
    """One turn of the agent loop, kept for the audit trail."""

    number: int
    ok: bool
    failures: list[str] = field(default_factory=list)
    llm: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"attempt": self.number, "ok": self.ok, "failures": self.failures, "llm": self.llm}


@dataclass
class ExtractionResult:
    spec: PipelineSpec | None
    attempts: list[Attempt] = field(default_factory=list)
    assumptions: list[dict[str, Any]] = field(default_factory=list)
    unsupported: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.spec is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "attempts": [a.to_dict() for a in self.attempts],
            "assumptions": self.assumptions,
            "unsupported": self.unsupported,
            "error": self.error,
        }


class LLMExtractionAgent:
    """Turns an arbitrarily structured BRD into a validated PipelineSpec."""

    def __init__(
        self,
        provider: LLMProvider,
        config: AppConfig,
        pipeline_type: str = "GCS->00->01",
        max_attempts: int = 3,
        max_document_chars: int = 60_000,
    ) -> None:
        self.provider = provider
        self.config = config
        self.pipeline_type = pipeline_type
        self.max_attempts = max_attempts
        self.max_document_chars = max_document_chars

    def extract(
        self,
        document: BrdDocument,
        run_id: str | None = None,
        rule_hint: dict[str, Any] | None = None,
    ) -> ExtractionResult:
        text = self._document_text(document)
        result = ExtractionResult(spec=None)
        prompt = extraction_prompt(text, self.pipeline_type, rule_hint)
        previous_json = ""

        for number in range(1, self.max_attempts + 1):
            try:
                response = self.provider.complete(SYSTEM_EXTRACTOR, prompt)
                payload = extract_json(response.text)
                previous_json = json.dumps(payload)
            except LLMError as exc:
                result.attempts.append(Attempt(number, False, [f"llm_error: {exc}"]))
                result.error = str(exc)
                logger.warning("Extraction attempt %s failed: %s", number, exc)
                break

            result.assumptions = payload.get("assumptions") or []
            result.unsupported = payload.get("unsupported") or []

            try:
                spec = self._compile(payload, document, run_id)
            except (ValidationError, ValueError, KeyError) as exc:
                failures = _summarise_error(exc)
                result.attempts.append(Attempt(number, False, failures, response.to_dict()))
                logger.warning("Attempt %s did not compile: %s", number, failures[:3])
                prompt = repair_prompt(previous_json, failures, text)
                continue

            report = validate_spec(spec, self.config)
            failures = [f"{r.name}: {r.message}" for r in report.failures]
            result.attempts.append(Attempt(number, not failures, failures, response.to_dict()))

            if not failures:
                logger.info("LLM extraction succeeded on attempt %s", number)
                result.spec = spec
                return result

            logger.warning("Attempt %s failed %s validation check(s); requesting repair.", number, len(failures))
            prompt = repair_prompt(previous_json, failures, text)

        if result.error is None:
            last = result.attempts[-1].failures if result.attempts else ["no attempts were made"]
            result.error = f"LLM extraction did not converge after {self.max_attempts} attempts: {last[:5]}"
        return result

    # -- compilation --------------------------------------------------------
    def _compile(
        self, payload: dict[str, Any], document: BrdDocument, run_id: str | None
    ) -> PipelineSpec:
        """Turn a raw model proposal into a typed spec.

        Environment-critical values are taken from config when pinned, never
        from the model.
        """
        directives = payload.get("directives") or {}
        pipeline = self.config.pipeline

        historical = [normalize_run_id(r) for r in (payload.get("historical_runs") or []) if r]
        resolved = normalize_run_id(run_id) if run_id else (historical[-1] if historical else None)
        if not resolved:
            raise ValueError(
                "No run identifier: none supplied on the command line and none found in the BRD."
            )

        raw_tables = payload.get("tables") or []
        if not raw_tables:
            raise ValueError("The model returned no tables for this pipeline type.")

        tables = [self._compile_table(t, directives) for t in raw_tables]

        return PipelineSpec(
            environment=self.config.environment,
            project_id=self._pin("project_id", payload.get("project_id"), self.config.gcp.project_id),
            location=self.config.gcp.location,
            gcs_bucket=self._pin("gcs_bucket", payload.get("gcs_bucket"), self.config.gcs.bucket),
            dataset_00=self._pin("dataset_00", payload.get("dataset_00"), self.config.bigquery.dataset_00),
            dataset_01=self._pin("dataset_01", payload.get("dataset_01"), self.config.bigquery.dataset_01),
            run_id=resolved,
            run_date=parse_run_date(resolved),
            pipeline_type=self.pipeline_type,
            rerun_strategy=pipeline.rerun_strategy,
            schedule=payload.get("schedule"),
            historical_runs=historical,
            tables=tables,
            brd_source=document.path.as_posix(),
            parser=f"llm:{self.provider.name}",
            warnings=[
                f"{a.get('topic')}: {a.get('assumption')} (confidence {a.get('confidence')})"
                for a in (payload.get("assumptions") or [])
            ],
        )

    def _compile_table(self, raw: dict[str, Any], directives: dict[str, Any]) -> TableSpec:
        pipeline = self.config.pipeline
        table_00 = (raw.get("table_00") or "").strip()
        table_01 = (raw.get("table_01") or "").strip()
        if not table_00 or not table_01:
            raise ValueError(f"Table entry is missing table_00 or table_01: {raw!r}")

        columns = [self._compile_column(c) for c in (raw.get("columns") or [])]
        if not columns:
            raise ValueError(f"{table_00}: the model returned no columns.")

        names = {c.name for c in columns}
        # Audit columns are the pipeline's responsibility, never the model's.
        if pipeline.partition_field not in names:
            columns.append(ColumnSpec(
                name=pipeline.partition_field, type="DATE", nullable=False,
                description="Run identifier as a date.", is_metadata=True,
            ))
        if pipeline.source_file_field not in names:
            columns.append(ColumnSpec(
                name=pipeline.source_file_field, type="STRING",
                description="Source file this row came from.", is_metadata=True,
            ))
        load_field = directives.get("load_date_field") or pipeline.load_date_field
        if load_field not in names:
            columns.append(ColumnSpec(
                name=load_field, type="DATE", nullable=False,
                description="Date this row was loaded into 01.",
                is_metadata=True, added_at_load=True,
            ))

        available = {c.name for c in columns}
        cluster_by = [c for c in (directives.get("cluster_candidates") or []) if c in available]
        if not cluster_by:
            fallback = {"county": "countyFIPSCode", "state": "stateCode"}.get(raw.get("grain") or "")
            if fallback in available:
                cluster_by = [fallback]

        folder = (raw.get("gcs_folder") or table_00).strip()
        return TableSpec(
            source_name=raw.get("source_name") or table_00,
            grain=raw.get("grain") if raw.get("grain") in {"county", "state", "property", "other"} else "other",
            table_00=table_00,
            table_01=table_01,
            gcs_folder=folder,
            file_pattern=folder[3:] if folder.startswith("00_") else folder,
            schema=columns,
            partition_by=directives.get("partition_by") or pipeline.partition_field,
            cluster_by=cluster_by,
            source_format=raw.get("source_format") or pipeline.source_format,
            target_format=pipeline.target_format,
            rerun_strategy=pipeline.rerun_strategy,
            source_location_hint=raw.get("source_location_hint"),
            brd_section=raw.get("source_name"),
        )

    def _compile_column(self, raw: dict[str, Any]) -> ColumnSpec:
        name = (raw.get("name") or "").strip()
        if not name:
            raise ValueError(f"Column entry has no name: {raw!r}")
        preserve = bool(raw.get("preserve_leading_zeros"))
        if name in self.config.file_prep.leading_zero_columns:
            preserve = True
        # A zero-padded code stored as a number loses its leading zeros.
        column_type = "STRING" if preserve else normalize_bq_type(str(raw.get("type") or "STRING"))
        return ColumnSpec(
            name=name,
            type=column_type,
            description=raw.get("description"),
            source_header=name,
            preserve_leading_zeros=preserve,
        )

    def _pin(self, field_name: str, from_llm: Any, from_config: str | None) -> str:
        if from_config is not None:
            return from_config
        value = (from_llm or "").strip() if isinstance(from_llm, str) else None
        if not value:
            raise ValueError(
                f"{field_name}: not found in the BRD and not pinned in config."
            )
        return value

    def _document_text(self, document: BrdDocument) -> str:
        lines = [("  " * min(line.level, 8)) + line.text for line in document.lines if line.text.strip()]
        text = "\n".join(lines)
        if len(text) > self.max_document_chars:
            logger.warning(
                "BRD is %s chars; truncating to %s for the model.", len(text), self.max_document_chars
            )
            text = text[: self.max_document_chars] + "\n[... truncated ...]"
        return text


def _summarise_error(exc: Exception) -> list[str]:
    if isinstance(exc, ValidationError):
        return [f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()][:20]
    return [f"{type(exc).__name__}: {exc}"]


__all__ = ["LLMExtractionAgent", "ExtractionResult", "Attempt"]
