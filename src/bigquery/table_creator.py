"""BigQuery statement execution and 00/01 table creation.

Three execution modes:

``dry_run=True``  - nothing is sent to BigQuery unless credentials happen to be
                    available, in which case a *server-side* dry run validates
                    the SQL and reports bytes processed. Without credentials the
                    SQL is only logged.
``dry_run=False`` - statements execute for real.
Destructive SQL (``DELETE`` / ``TRUNCATE`` / ``DROP``) additionally requires
``allow_destructive=True``, which the approval gate sets.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from ..models.pipeline_spec import PipelineSpec, TableSpec
from ..utils.config import AppConfig
from ..utils.logging_setup import get_logger

logger = get_logger(__name__)

_DESTRUCTIVE = re.compile(r"\b(DELETE\s+FROM|TRUNCATE\s+TABLE|DROP\s+TABLE|DROP\s+SCHEMA)\b", re.IGNORECASE)


class DestructiveStatementBlocked(RuntimeError):
    """Raised when destructive SQL is submitted without explicit approval."""


@dataclass
class JobResult:
    label: str
    statement_preview: str
    status: str                      # SIMULATED | VALIDATED | SUCCESS | FAILED | BLOCKED
    job_id: str | None = None
    bytes_processed: int | None = None
    rows_affected: int | None = None
    error: str | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status in ("SIMULATED", "VALIDATED", "SUCCESS")

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "status": self.status,
            "job_id": self.job_id,
            "bytes_processed": self.bytes_processed,
            "rows_affected": self.rows_affected,
            "error": self.error,
            "statement_preview": self.statement_preview,
        }


@dataclass
class BigQueryExecutor:
    config: AppConfig
    dry_run: bool = True
    allow_destructive: bool = False
    audit: list[dict[str, Any]] = field(default_factory=list)
    _client: Any = None
    _client_failed: bool = False

    # -- client -------------------------------------------------------------
    def client(self):
        if self._client is None and not self._client_failed:
            from google.cloud import bigquery

            from ..file_prep.gcs_uploader import _impersonated_credentials

            self._client = bigquery.Client(
                project=self.config.gcp.project_id,
                location=self.config.gcp.location,
                credentials=_impersonated_credentials(self.config),
            )
            logger.info(
                "Connected to BigQuery: project=%s location=%s",
                self.config.gcp.project_id, self.config.gcp.location,
            )
        return self._client

    def _try_client(self):
        """Return a client, or ``None`` when credentials are unavailable."""
        try:
            return self.client()
        except Exception as exc:  # noqa: BLE001 - offline dry runs must not crash
            self._client_failed = True
            logger.warning("BigQuery client unavailable (%s). Running fully offline.", exc)
            return None

    # -- execution ----------------------------------------------------------
    def execute(self, sql: str, label: str, *, expects_rows: bool = False) -> JobResult:
        preview = _preview(sql)
        destructive = bool(_DESTRUCTIVE.search(sql))

        if destructive and not self.allow_destructive:
            message = (
                f"BLOCKED destructive statement '{label}'. "
                "Approve the run (--require-approval + token) or set safety.allow_destructive."
            )
            logger.error(message)
            result = JobResult(label, preview, "BLOCKED", error=message)
            return self._record(result)

        if self.dry_run:
            return self._record(self._dry_run(sql, label, preview))

        client = self.client()
        from google.cloud import bigquery

        logger.info("Executing '%s'", label)
        job = client.query(sql, job_config=bigquery.QueryJobConfig(labels=_labels(label)))
        try:
            iterator = job.result()
        except Exception as exc:  # noqa: BLE001 - surface the BigQuery error in the report
            logger.error("Job %s failed: %s", job.job_id, exc)
            return self._record(JobResult(label, preview, "FAILED", job_id=job.job_id, error=str(exc)))

        rows = [dict(row) for row in iterator] if expects_rows else []
        result = JobResult(
            label, preview, "SUCCESS",
            job_id=job.job_id,
            bytes_processed=job.total_bytes_processed,
            rows_affected=job.num_dml_affected_rows,
            rows=rows,
        )
        logger.info(
            "Job %s OK (%s bytes, %s rows affected)",
            job.job_id, job.total_bytes_processed, job.num_dml_affected_rows,
        )
        return self._record(result)

    def _dry_run(self, sql: str, label: str, preview: str) -> JobResult:
        client = self._try_client()
        if client is None:
            logger.info("[DRY RUN offline] %s\n%s", label, sql)
            return JobResult(label, preview, "SIMULATED")

        from google.cloud import bigquery

        config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False, labels=_labels(label))
        try:
            job = client.query(sql, job_config=config)
        except Exception as exc:  # noqa: BLE001
            logger.error("[DRY RUN] %s is INVALID: %s", label, exc)
            return JobResult(label, preview, "FAILED", error=str(exc))
        logger.info("[DRY RUN] %s validated by BigQuery (%s bytes would be processed)",
                    label, job.total_bytes_processed)
        return JobResult(label, preview, "VALIDATED", bytes_processed=job.total_bytes_processed)

    def query_rows(self, sql: str, label: str) -> list[dict[str, Any]]:
        return self.execute(sql, label, expects_rows=True).rows

    def _record(self, result: JobResult) -> JobResult:
        self.audit.append(result.to_dict())
        return result


class TableCreator:
    """Creates the 00 external tables and the 01 managed tables."""

    def __init__(self, executor: BigQueryExecutor) -> None:
        self.executor = executor

    def ensure_datasets(self, spec: PipelineSpec) -> list[JobResult]:
        results = []
        for dataset in (spec.dataset_00, spec.dataset_01):
            sql = (
                f"CREATE SCHEMA IF NOT EXISTS `{spec.project_id}.{dataset}`\n"
                f"OPTIONS (location = '{spec.location}');"
            )
            results.append(self.executor.execute(sql, f"create_dataset_{dataset}"))
        return results

    def create_00(self, table: TableSpec, sql: str) -> JobResult:
        return self.executor.execute(sql, f"create_00_{table.table_00}")

    def create_01(self, table: TableSpec, sql: str) -> JobResult:
        return self.executor.execute(sql, f"create_01_{table.table_01}")


def _labels(label: str) -> dict[str, str]:
    """Resource attribution labels applied to every job."""
    safe = re.sub(r"[^a-z0-9_-]", "_", label.lower())[:63]
    return {"framework": "brd_to_bigquery_agent", "step": safe}


def _preview(sql: str, limit: int = 400) -> str:
    condensed = re.sub(r"\n\s*\n", "\n", sql.strip())
    return condensed if len(condensed) <= limit else condensed[:limit] + " ..."


__all__ = ["BigQueryExecutor", "JobResult", "TableCreator", "DestructiveStatementBlocked"]
