"""LLM and agentic decision layer.

Imports are lazy so the pipeline runs fully offline when no LLM is configured.
"""

from __future__ import annotations

__all__ = [
    "get_provider", "LLMProvider", "LLMError", "EchoProvider",
    "LLMExtractionAgent", "ExtractionResult",
    "DecisionAgent", "Decision",
]


def __getattr__(name: str):
    if name in {"get_provider", "LLMProvider", "LLMError", "EchoProvider"}:
        from . import provider

        return getattr(provider, name)
    if name in {"LLMExtractionAgent", "ExtractionResult"}:
        from . import extraction_agent

        return getattr(extraction_agent, name)
    if name in {"DecisionAgent", "Decision"}:
        from . import decision_agent

        return getattr(decision_agent, name)
    raise AttributeError(name)
