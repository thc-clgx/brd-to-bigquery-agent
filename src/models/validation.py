"""Shared validation result types used by spec, file and BigQuery checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Status = Literal["PASS", "WARN", "FAIL", "SKIPPED"]


@dataclass
class ValidationResult:
    name: str
    status: Status
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status in ("PASS", "WARN", "SKIPPED")

    def __str__(self) -> str:
        return f"[{self.status:7}] {self.name}: {self.message}"


@dataclass
class ValidationReport:
    results: list[ValidationResult] = field(default_factory=list)

    def add(self, result: ValidationResult) -> ValidationResult:
        self.results.append(result)
        return result

    def add_result(self, name: str, condition: bool, ok_message: str, fail_message: str,
                   severity: Status = "FAIL", **details: Any) -> ValidationResult:
        return self.add(
            ValidationResult(
                name=name,
                status="PASS" if condition else severity,
                message=ok_message if condition else fail_message,
                details=details,
            )
        )

    def extend(self, results: list[ValidationResult]) -> None:
        self.results.extend(results)

    @property
    def failures(self) -> list[ValidationResult]:
        return [r for r in self.results if r.status == "FAIL"]

    @property
    def warnings(self) -> list[ValidationResult]:
        return [r for r in self.results if r.status == "WARN"]

    @property
    def passed(self) -> bool:
        return not self.failures

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "total": len(self.results),
            "failed": len(self.failures),
            "warnings": len(self.warnings),
            "results": [
                {"name": r.name, "status": r.status, "message": r.message, "details": r.details}
                for r in self.results
            ],
        }

    def raise_if_failed(self, context: str = "validation") -> None:
        if self.failures:
            lines = "\n".join(f"  - {r.name}: {r.message}" for r in self.failures)
            raise ValueError(f"{context} failed with {len(self.failures)} error(s):\n{lines}")
