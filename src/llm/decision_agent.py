"""Narrow LLM decisions with confidence, provenance and escalation.

Full-document extraction is one use of a model; the other is resolving specific
ambiguities the rule engine cannot. Each decision records what was asked, what
was answered, how confident the model was, and which layer produced it - so a
reviewer can see exactly where judgement was applied.

Decisions below the confidence threshold are marked ``needs_review`` and are
surfaced at the approval gate rather than being applied silently.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..utils.logging_setup import get_logger
from .provider import LLMProvider, LLMError, extract_json
from .prompts import SYSTEM_DECIDER, SYSTEM_MAPPER, decision_prompt, mapping_prompt

logger = get_logger(__name__)

DEFAULT_THRESHOLD = 0.7


@dataclass
class Decision:
    question: str
    answer: Any
    confidence: float
    rationale: str
    source: str  # rule | llm | config | fallback
    needs_review: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "question": self.question,
            "answer": self.answer,
            "confidence": round(self.confidence, 2),
            "rationale": self.rationale,
            "source": self.source,
            "needs_review": self.needs_review,
        }


class DecisionAgent:
    """Asks the model narrow questions; falls back to rules when unavailable."""

    def __init__(
        self,
        provider: LLMProvider,
        threshold: float = DEFAULT_THRESHOLD,
        enabled: bool = True,
    ) -> None:
        self.provider = provider
        self.threshold = threshold
        self.enabled = enabled and provider.available()
        self.decisions: list[Decision] = []

    @property
    def needs_review(self) -> list[Decision]:
        return [d for d in self.decisions if d.needs_review]

    def decide(
        self,
        question: str,
        context: dict[str, Any],
        options: list[str] | None = None,
        fallback: Any = None,
        fallback_rationale: str = "rule-based default",
    ) -> Decision:
        """Answer one question, degrading to ``fallback`` when the LLM is off."""
        if not self.enabled:
            return self._record(Decision(
                question, fallback, 1.0 if fallback is not None else 0.0,
                fallback_rationale, "rule", needs_review=fallback is None,
            ))

        try:
            response = self.provider.complete(SYSTEM_DECIDER, decision_prompt(question, context, options))
            payload = extract_json(response.text)
        except LLMError as exc:
            logger.warning("Decision failed (%s); using fallback for: %s", exc, question)
            return self._record(Decision(
                question, fallback, 0.0, f"llm unavailable: {exc}", "fallback",
                needs_review=True,
            ))

        answer = payload.get("answer", fallback)
        confidence = _confidence(payload.get("confidence"))
        rationale = str(payload.get("rationale") or "")

        if options and answer not in options:
            logger.warning("Model answered %r, which is not an allowed option; using fallback.", answer)
            return self._record(Decision(
                question, fallback, 0.0, f"invalid answer {answer!r}", "fallback", needs_review=True,
            ))

        return self._record(Decision(
            question, answer, confidence, rationale, "llm",
            needs_review=confidence < self.threshold,
        ))

    def map_headers(
        self,
        source_name: str,
        spec_columns: list[dict[str, Any]],
        headers: list[str],
        fallback: dict[str, Any] | None = None,
    ) -> Decision:
        """Semantically map physical file headers onto spec column names.

        Used when deterministic normalisation leaves columns unmatched - the
        model understands that ``ttl_acr_chg`` means ``totalAcreageChange``
        where string similarity does not.
        """
        question = f"Map the staged file headers for {source_name!r} onto the specification columns."
        if not self.enabled:
            return self._record(Decision(
                question, fallback or {}, 1.0, "deterministic header matching", "rule",
            ))

        try:
            response = self.provider.complete(
                SYSTEM_MAPPER, mapping_prompt(spec_columns, headers, source_name)
            )
            payload = extract_json(response.text)
        except LLMError as exc:
            return self._record(Decision(
                question, fallback or {}, 0.0, f"llm unavailable: {exc}", "fallback", needs_review=True,
            ))

        mapping = {k: v for k, v in (payload.get("mapping") or {}).items() if v in headers}
        duplicates = len(mapping.values()) != len(set(mapping.values()))
        confidence = _confidence(payload.get("confidence"))
        if duplicates:
            # Two spec columns bound to one header would corrupt data silently.
            logger.warning("%s: model mapped two columns to the same header; rejecting.", source_name)
            return self._record(Decision(
                question, fallback or {}, 0.0, "mapping was not one-to-one", "fallback", needs_review=True,
            ))

        answer = {
            "mapping": mapping,
            "missing": payload.get("missing") or [],
            "extra": payload.get("extra") or [],
        }
        return self._record(Decision(
            question, answer, confidence, str(payload.get("rationale") or ""), "llm",
            needs_review=confidence < self.threshold,
        ))

    def _record(self, decision: Decision) -> Decision:
        self.decisions.append(decision)
        level = logger.warning if decision.needs_review else logger.info
        level(
            "DECISION [%s conf=%.2f] %s -> %r",
            decision.source, decision.confidence, decision.question[:70], decision.answer,
        )
        return decision

    def to_list(self) -> list[dict[str, Any]]:
        return [d.to_dict() for d in self.decisions]


def _confidence(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return 0.0


__all__ = ["DecisionAgent", "Decision", "DEFAULT_THRESHOLD"]
