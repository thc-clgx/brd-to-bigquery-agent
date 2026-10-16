"""Human-in-the-loop approval gate.

Rejection is the default. An action proceeds only when a human types the exact
token from ``safety.approval_token`` (or supplies it via ``--approval-token`` /
the ``BRD_AGENT_APPROVAL_TOKEN`` environment variable, e.g. for CI).

Airflow integration point: :meth:`ApprovalGate.airflow_callable` returns a
callable suitable for a ``PythonOperator``. Replace :func:`_external_approval`
with a Jira transition check, Teams adaptive-card response or GitHub PR review
when that plumbing exists.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from ..models.pipeline_spec import PipelineSpec
from ..utils.config import AppConfig
from ..utils.logging_setup import get_logger

logger = get_logger(__name__)

ENV_TOKEN = "BRD_AGENT_APPROVAL_TOKEN"


class ApprovalRejected(RuntimeError):
    """Raised when a gated action is not approved."""


@dataclass
class ApprovalDecision:
    action: str
    approved: bool
    approver: str
    method: str
    timestamp: str
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "approved": self.approved,
            "approver": self.approver,
            "method": self.method,
            "timestamp": self.timestamp,
            "reason": self.reason,
        }


@dataclass
class ApprovalGate:
    config: AppConfig
    dry_run: bool = True
    interactive: bool = True
    supplied_token: str | None = None
    decisions: list[ApprovalDecision] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.supplied_token = self.supplied_token or os.environ.get(ENV_TOKEN)

    # -- public API ---------------------------------------------------------
    def require(self, action: str, summary: dict[str, Any], destructive: bool = False) -> ApprovalDecision:
        """Gate ``action``. Raises :class:`ApprovalRejected` unless approved."""
        if self.dry_run:
            return self._record(ApprovalDecision(
                action, True, _who(), "auto:dry_run", _now(),
                "dry run - nothing is written, approval not required",
            ))

        if not self.config.safety.approval_required:
            logger.warning("approval_required=false in config; auto-approving %r.", action)
            return self._record(ApprovalDecision(action, True, _who(), "auto:config", _now(),
                                                 "safety.approval_required is false"))

        self._print_summary(action, summary, destructive)
        expected = self.config.safety.approval_token

        if self.supplied_token:
            approved = self.supplied_token == expected
            decision = ApprovalDecision(
                action, approved, _who(), "token", _now(),
                "token accepted" if approved else "token did not match safety.approval_token",
            )
            self._record(decision)
            if not approved:
                raise ApprovalRejected(f"Approval token rejected for {action!r}.")
            logger.info("Action %r approved via supplied token.", action)
            return decision

        if not self.interactive or not sys.stdin.isatty():
            decision = self._record(ApprovalDecision(
                action, False, _who(), "none", _now(),
                "no approval token supplied and the session is non-interactive",
            ))
            raise ApprovalRejected(
                f"{action!r} requires approval. Re-run with --approval-token {expected} "
                f"or set {ENV_TOKEN}."
            )

        try:
            answer = input(f"\nType {expected} to approve '{action}' (anything else rejects): ").strip()
        except EOFError:
            answer = ""

        approved = answer == expected
        decision = self._record(ApprovalDecision(
            action, approved, _who(), "interactive", _now(),
            "operator approved" if approved else "operator rejected or entered the wrong token",
        ))
        if not approved:
            raise ApprovalRejected(f"Operator did not approve {action!r}.")
        logger.info("Action %r approved interactively by %s.", action, decision.approver)
        return decision

    def airflow_callable(self, action: str, summary: dict[str, Any]) -> Callable[..., dict[str, Any]]:
        """Build a PythonOperator callable for an Airflow approval task."""

        def _task(**context: Any) -> dict[str, Any]:
            params = (context.get("params") or {}) if context else {}
            token = params.get("approval_token") or os.environ.get(ENV_TOKEN)
            self.supplied_token = token
            self.interactive = False
            decision = self.require(action, summary)
            return decision.to_dict()

        _task.__name__ = f"approval_gate_{action}"
        return _task

    # -- helpers ------------------------------------------------------------
    def _print_summary(self, action: str, summary: dict[str, Any], destructive: bool) -> None:
        border = "=" * 78
        lines = [border, f"APPROVAL REQUIRED: {action}", border]
        if destructive:
            lines.append("!! DESTRUCTIVE: existing rows for this run_date will be DELETED before reload.")
        lines.append(f"environment      : {self.config.environment}")
        lines.append(f"project          : {self.config.gcp.project_id}")
        for key, value in summary.items():
            lines.append(f"{key:<17}: {value}")
        lines.append(border)
        print("\n".join(lines))

    def _record(self, decision: ApprovalDecision) -> ApprovalDecision:
        self.decisions.append(decision)
        return decision


def summarize_spec(spec: PipelineSpec) -> dict[str, Any]:
    """Standard approval summary for a pipeline spec."""
    return {
        "brd": spec.brd_source,
        "run_id": spec.run_id,
        "run_date": str(spec.run_date),
        "gcs_bucket": spec.gcs_bucket,
        "dataset_00": spec.dataset_00,
        "dataset_01": spec.dataset_01,
        "rerun_strategy": spec.rerun_strategy,
        "tables": ", ".join(t.table_01 for t in spec.tables),
    }


def _who() -> str:
    return os.environ.get("USERNAME") or os.environ.get("USER") or "unknown"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


__all__ = ["ApprovalGate", "ApprovalDecision", "ApprovalRejected", "summarize_spec", "ENV_TOKEN"]
