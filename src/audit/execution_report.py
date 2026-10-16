"""Collects everything that happened during a run and writes JSON + Markdown."""

from __future__ import annotations

import json
import platform
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..models.pipeline_spec import PipelineSpec
from ..models.validation import ValidationReport
from ..utils.config import PROJECT_ROOT
from ..utils.logging_setup import get_logger

logger = get_logger(__name__)


@dataclass
class ExecutionReport:
    run_id: str
    environment: str
    dry_run: bool
    started_at: str = field(default_factory=lambda: _now())
    finished_at: str | None = None
    status: str = "RUNNING"
    steps: list[dict[str, Any]] = field(default_factory=list)
    spec_path: str | None = None
    # How the BRD was read: strategy, LLM attempts, and every judgement call.
    extraction: dict[str, Any] = field(default_factory=dict)
    source_profiles: dict[str, Any] = field(default_factory=dict)
    generated_sql: list[str] = field(default_factory=list)
    approvals: list[dict[str, Any]] = field(default_factory=list)
    gcs_uploads: list[dict[str, Any]] = field(default_factory=list)
    bigquery_jobs: list[dict[str, Any]] = field(default_factory=list)
    validations: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    # -- recording ----------------------------------------------------------
    def step(self, name: str, status: str, **details: Any) -> None:
        entry = {"name": name, "status": status, "at": _now(), **details}
        self.steps.append(entry)
        logger.info("STEP %-28s %s", name, status)

    def add_error(self, message: str) -> None:
        self.errors.append(message)
        logger.error(message)

    def attach_spec(self, spec: PipelineSpec, spec_path: Path) -> None:
        self.spec_path = str(spec_path)
        self.step(
            "extract_pipeline_spec", "OK",
            tables=[t.table_01 for t in spec.tables],
            run_date=str(spec.run_date),
            warnings=spec.warnings,
        )

    def attach_validations(self, name: str, report: ValidationReport) -> None:
        self.validations[name] = report.to_dict()
        self.step(f"validate_{name}", "OK" if report.passed else "FAILED",
                  failed=len(report.failures), warnings=len(report.warnings))

    def finish(self, status: str | None = None) -> None:
        self.finished_at = _now()
        if status:
            self.status = status
        elif self.errors:
            self.status = "FAILED"
        else:
            failed = any(v.get("failed") for v in self.validations.values())
            self.status = "COMPLETED_WITH_FAILURES" if failed else "SUCCESS"

    # -- output -------------------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "environment": self.environment,
            "dry_run": self.dry_run,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "host": platform.node(),
            "spec_path": self.spec_path,
            "extraction": self.extraction,
            "source_profiles": self.source_profiles,
            "generated_sql": self.generated_sql,
            "approvals": self.approvals,
            "gcs_uploads": self.gcs_uploads,
            "bigquery_jobs": self.bigquery_jobs,
            "validations": self.validations,
            "steps": self.steps,
            "errors": self.errors,
        }

    def save(self, directory: str | Path) -> Path:
        out_dir = Path(directory)
        if not out_dir.is_absolute():
            out_dir = PROJECT_ROOT / out_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        base = f"execution_report_{self.environment}_{self.run_id}_{stamp}"

        json_path = out_dir / f"{base}.json"
        json_path.write_text(json.dumps(self.to_dict(), indent=2, default=str), encoding="utf-8")

        md_path = out_dir / f"{base}.md"
        md_path.write_text(self.to_markdown(), encoding="utf-8")

        logger.info("Execution report -> %s", json_path)
        return json_path

    def to_markdown(self) -> str:
        lines = [
            f"# Execution report - {self.run_id} ({self.environment})",
            "",
            f"- **Status**: {self.status}",
            f"- **Dry run**: {self.dry_run}",
            f"- **Started**: {self.started_at}",
            f"- **Finished**: {self.finished_at}",
            f"- **Spec**: {self.spec_path}",
            "",
            "## Steps",
            "",
            "| Step | Status | At |",
            "| --- | --- | --- |",
        ]
        lines += [f"| {s['name']} | {s['status']} | {s['at']} |" for s in self.steps]

        if self.extraction:
            lines += [
                "", "## How the BRD was read", "",
                f"- **Strategy**: {self.extraction.get('strategy')}",
            ]
            if llm := self.extraction.get("llm"):
                lines.append(f"- **LLM attempts**: {len(llm.get('attempts') or [])}")
                for assumption in llm.get("assumptions") or []:
                    lines.append(
                        f"  - _assumption_ {assumption.get('topic')}: {assumption.get('assumption')} "
                        f"(confidence {assumption.get('confidence')})"
                    )
                for item in llm.get("unsupported") or []:
                    lines.append(f"  - _unsupported_ {item}")
            if decisions := self.extraction.get("decisions"):
                lines += [
                    "", "### Decisions", "",
                    "| Question | Answer | Source | Confidence | Review |",
                    "| --- | --- | --- | --- | --- |",
                ]
                lines += [
                    f"| {d['question']} | {d['answer']} | {d['source']} | {d['confidence']} |"
                    f" {'YES' if d['needs_review'] else ''} |"
                    for d in decisions
                ]

        if self.approvals:
            lines += ["", "## Approvals", "", "| Action | Approved | Approver | Method |", "| --- | --- | --- | --- |"]
            lines += [
                f"| {a['action']} | {a['approved']} | {a['approver']} | {a['method']} |"
                for a in self.approvals
            ]

        if self.generated_sql:
            lines += ["", "## Generated SQL", ""] + [f"- `{p}`" for p in self.generated_sql]

        if self.gcs_uploads:
            lines += ["", "## GCS uploads", "", "| Object | Uploaded | Bytes |", "| --- | --- | --- |"]
            lines += [
                f"| {u['gcs_uri']} | {u['uploaded']} | {u['size_bytes']} |" for u in self.gcs_uploads
            ]

        if self.bigquery_jobs:
            lines += ["", "## BigQuery jobs", "", "| Label | Status | Job id | Rows affected |", "| --- | --- | --- | --- |"]
            lines += [
                f"| {j['label']} | {j['status']} | {j.get('job_id') or '-'} | {j.get('rows_affected') if j.get('rows_affected') is not None else '-'} |"
                for j in self.bigquery_jobs
            ]

        for name, payload in self.validations.items():
            lines += [
                "", f"## Validations - {name}", "",
                f"{payload['total']} checks, {payload['failed']} failed, {payload['warnings']} warnings",
                "", "| Check | Status | Message |", "| --- | --- | --- |",
            ]
            lines += [
                f"| {r['name']} | {r['status']} | {r['message']} |"
                for r in payload["results"]
                if r["status"] != "PASS"
            ] or ["| _all checks passed_ |  |  |"]

        if self.errors:
            lines += ["", "## Errors", ""] + [f"- {e}" for e in self.errors]

        return "\n".join(lines) + "\n"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


__all__ = ["ExecutionReport"]
