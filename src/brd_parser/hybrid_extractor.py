"""Hybrid extraction: deterministic rules first, LLM for the gaps.

Rules are exact, cheap and reproducible - but they only recognise the document
shapes they were written for. An LLM reads any shape but is not reproducible.
Running the rules first and using the model to confirm, correct and complete
gives both: the common case costs nothing, and an unfamiliar BRD still works.

Strategies
  rule_based  rules only; the model is never called
  llm         the model extracts from scratch, rules supply a hint
  hybrid      rules first; the model is called only if the rules fail or the
              result does not pass validation

Whatever the path, the output is a ``PipelineSpec`` that has passed the same
deterministic validator. The model cannot widen what the pipeline will accept.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..llm.decision_agent import DecisionAgent
from ..llm.extraction_agent import ExtractionResult, LLMExtractionAgent
from ..llm.provider import LLMProvider, LLMUnavailable, NullProvider, get_provider
from ..models.pipeline_spec import PipelineSpec
from ..utils.config import AppConfig
from ..utils.logging_setup import get_logger
from .document_reader import BrdDocument
from .requirement_extractor import BrdParseError, RequirementExtractor
from .spec_validator import validate_spec

logger = get_logger(__name__)


@dataclass
class HybridResult:
    spec: PipelineSpec
    strategy: str
    rule_error: str | None = None
    llm_result: ExtractionResult | None = None
    decisions: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy,
            "rule_error": self.rule_error,
            "llm": self.llm_result.to_dict() if self.llm_result else None,
            "decisions": self.decisions,
            "notes": self.notes,
        }


class HybridExtractor:
    """Chooses between the rule engine and the LLM agent, and records why."""

    def __init__(
        self,
        config: AppConfig,
        pipeline_type: str = "GCS->00->01",
        mode: str | None = None,
        provider: LLMProvider | None = None,
    ) -> None:
        self.config = config
        self.pipeline_type = pipeline_type
        llm_config = config.llm
        # An explicit mode or injected provider is itself an opt-in, so it
        # overrides llm.enabled. Otherwise the config decides.
        opted_in = mode is not None or provider is not None
        self.mode = mode or llm_config.extraction_mode
        if not llm_config.enabled and not opted_in and self.mode != "rule_based":
            logger.info("LLM is disabled in config; extraction falls back to rules.")
            self.mode = "rule_based"

        self.provider = provider or (
            get_provider(
                llm_config.provider,
                model=llm_config.model,
                timeout=llm_config.timeout_seconds,
                command=llm_config.cli_command or None,
            )
            if self.mode != "rule_based"
            else NullProvider()
        )
        if self.mode != "rule_based" and not self.provider.available():
            if self.mode == "llm":
                raise LLMUnavailable(
                    "LLM-first extraction requires an available provider; refusing a rules-only fallback."
                )
            logger.warning("No LLM provider available; extraction falls back to rules.")
            self.mode = "rule_based"

        self.decision_agent = DecisionAgent(
            self.provider,
            threshold=llm_config.min_confidence,
            enabled=self.mode != "rule_based",
        )

    def extract(self, document: BrdDocument, run_id: str | None = None) -> HybridResult:
        if self.mode == "rule_based":
            return self._rules_only(document, run_id)
        if self.mode == "llm":
            return self._llm_first(document, run_id)
        return self._hybrid(document, run_id)

    # -- strategies ---------------------------------------------------------
    def _rules_only(self, document: BrdDocument, run_id: str | None) -> HybridResult:
        spec = RequirementExtractor(self.config, self.pipeline_type).extract(document, run_id)
        return HybridResult(spec=spec, strategy="rule_based")

    def _llm_first(self, document: BrdDocument, run_id: str | None) -> HybridResult:
        hint, rule_error = self._rule_hint(document, run_id)
        result = self._run_agent(document, run_id, hint)
        if not result.ok or result.spec is None:
            raise BrdParseError(result.error or "LLM extraction failed.")
        return HybridResult(
            spec=result.spec, strategy="llm", rule_error=rule_error,
            llm_result=result, decisions=self.decision_agent.to_list(),
        )

    def _hybrid(self, document: BrdDocument, run_id: str | None) -> HybridResult:
        notes: list[str] = []
        rule_spec: PipelineSpec | None = None
        rule_error: str | None = None

        try:
            rule_spec = RequirementExtractor(self.config, self.pipeline_type).extract(document, run_id)
        except (BrdParseError, ValueError) as exc:
            rule_error = f"{type(exc).__name__}: {exc}"
            logger.warning("Rule-based extraction failed: %s", rule_error)

        if rule_spec is not None:
            report = validate_spec(rule_spec, self.config)
            if report.passed:
                notes.append("Rule-based extraction passed validation; the LLM was not needed.")
                logger.info(notes[-1])
                return HybridResult(spec=rule_spec, strategy="hybrid:rules", notes=notes)
            rule_error = "; ".join(f"{r.name}: {r.message}" for r in report.failures)[:800]
            notes.append(f"Rule-based spec failed {len(report.failures)} check(s); escalating to the LLM.")
            logger.warning(notes[-1])

        hint = _hint_from_spec(rule_spec) if rule_spec is not None else None
        result = self._run_agent(document, run_id, hint)

        if result.ok and result.spec is not None:
            notes.append(f"LLM produced a valid spec after {len(result.attempts)} attempt(s).")
            return HybridResult(
                spec=result.spec, strategy="hybrid:llm", rule_error=rule_error,
                llm_result=result, decisions=self.decision_agent.to_list(), notes=notes,
            )

        if rule_spec is not None:
            # A rule spec that fails validation is still better than nothing;
            # the runner's own validation stage remains the real gate.
            notes.append(f"LLM did not converge ({result.error}); keeping the rule-based spec.")
            logger.warning(notes[-1])
            return HybridResult(
                spec=rule_spec, strategy="hybrid:rules_fallback", rule_error=rule_error,
                llm_result=result, decisions=self.decision_agent.to_list(), notes=notes,
            )

        raise BrdParseError(
            f"Neither extractor produced a usable spec. Rules: {rule_error}. LLM: {result.error}"
        )

    # -- helpers ------------------------------------------------------------
    def _run_agent(
        self, document: BrdDocument, run_id: str | None, hint: dict[str, Any] | None
    ) -> ExtractionResult:
        agent = LLMExtractionAgent(
            self.provider,
            self.config,
            pipeline_type=self.pipeline_type,
            max_attempts=self.config.llm.max_attempts,
            max_document_chars=self.config.llm.max_document_chars,
        )
        return agent.extract(document, run_id, hint)

    def _rule_hint(
        self, document: BrdDocument, run_id: str | None
    ) -> tuple[dict[str, Any] | None, str | None]:
        try:
            spec = RequirementExtractor(self.config, self.pipeline_type).extract(document, run_id)
        except (BrdParseError, ValueError) as exc:
            return None, f"{type(exc).__name__}: {exc}"
        return _hint_from_spec(spec), None


def _hint_from_spec(spec: PipelineSpec) -> dict[str, Any]:
    """Condense a spec into the compact shape the extraction prompt expects."""
    return {
        "dataset_00": spec.dataset_00,
        "dataset_01": spec.dataset_01,
        "schedule": spec.schedule,
        "historical_runs": spec.historical_runs,
        "tables": [
            {
                "source_name": table.source_name,
                "table_00": table.table_00,
                "table_01": table.table_01,
                "grain": table.grain,
                "columns": [
                    {"name": c.name, "type": c.type, "preserve_leading_zeros": c.preserve_leading_zeros}
                    for c in table.schema_columns
                    if not c.is_metadata
                ],
            }
            for table in spec.tables
        ],
    }


__all__ = ["HybridExtractor", "HybridResult"]
