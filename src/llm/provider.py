"""Pluggable LLM providers.

The framework never depends on a specific vendor. A provider only has to turn
a (system, user) prompt pair into text; everything above this module works with
the resulting JSON. Providers are resolved at runtime, so the same code runs
with GitHub Copilot CLI today and an API key later without changes.

Nothing here reads credentials from config files. API keys come from the
environment only, and the CLI providers rely on the user's existing login.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from ..utils.logging_setup import get_logger

logger = get_logger(__name__)


class LLMError(RuntimeError):
    """Raised when a provider cannot produce a response."""


class LLMUnavailable(LLMError):
    """Raised when a provider is not installed, not logged in, or has no key."""


class LLMRequestError(LLMError):
    """Safe request diagnostics without upstream messages or credentials."""

    def __init__(self, error_type: str, status_code: int | None = None) -> None:
        self.error_type = error_type
        self.status_code = status_code
        super().__init__(
            f"LLM request failed ({error_type}); sensitive details were omitted."
        )


@dataclass
class LLMResponse:
    text: str
    provider: str
    model: str
    latency_ms: int = 0
    tokens_in: int | None = None
    tokens_out: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "latency_ms": self.latency_ms,
            "tokens_in": self.tokens_in,
            "tokens_out": self.tokens_out,
            "chars": len(self.text),
        }


class LLMProvider(ABC):
    """Minimal contract every provider implements."""

    name: str = "abstract"

    def __init__(self, model: str = "", timeout: int = 180, **options: Any) -> None:
        self.model = model
        self.timeout = timeout
        self.options = options

    @abstractmethod
    def available(self) -> bool:
        """True when this provider can actually be called right now."""

    @abstractmethod
    def _complete(self, system: str, user: str, temperature: float) -> LLMResponse:
        ...

    def complete(self, system: str, user: str, temperature: float = 0.0) -> LLMResponse:
        if not self.available():
            raise LLMUnavailable(f"LLM provider {self.name!r} is not available.")
        started = time.monotonic()
        response = self._complete(system, user, temperature)
        response.latency_ms = int((time.monotonic() - started) * 1000)
        logger.info(
            "LLM %s/%s responded in %sms (%s chars)",
            response.provider, response.model, response.latency_ms, len(response.text),
        )
        return response


class NullProvider(LLMProvider):
    """Stand-in when no LLM is configured; callers fall back to rules."""

    name = "none"

    def available(self) -> bool:
        return False

    def _complete(self, system: str, user: str, temperature: float) -> LLMResponse:
        raise LLMUnavailable("No LLM provider is configured.")


class CopilotCliProvider(LLMProvider):
    """Drives a locally installed Copilot-style CLI in non-interactive mode.

    The command is configurable because CLI surfaces change; ``{prompt}`` is
    substituted into the argument list. Defaults suit the standalone
    ``copilot -p`` CLI.
    """

    name = "copilot_cli"
    DEFAULT_COMMAND = ["copilot", "-p", "{prompt}", "--allow-all-tools"]
    # Windows caps a command line at 32767 chars; stay well clear.
    MAX_ARG_CHARS = 8000
    # Output that means the shim on PATH is an installer stub, not the CLI.
    _STUB_MARKERS = ("cannot find github copilot cli", "install github copilot cli")

    def __init__(self, model: str = "", timeout: int = 300, **options: Any) -> None:
        super().__init__(model, timeout, **options)
        self.command: list[str] = list(options.get("command") or self.DEFAULT_COMMAND)
        self._broken = False

    def available(self) -> bool:
        return not self._broken and shutil.which(self.command[0]) is not None

    def _launcher(self) -> list[str]:
        """Resolve the executable, wrapping .bat/.cmd/.ps1 in their interpreter.

        Windows shims Node CLIs as batch files, which CreateProcess cannot
        execute directly.
        """
        resolved = shutil.which(self.command[0])
        if not resolved:
            raise LLMUnavailable(f"{self.command[0]!r} is not on PATH.")
        suffix = os.path.splitext(resolved)[1].lower()
        if suffix in {".bat", ".cmd"}:
            return ["cmd", "/c", resolved]
        if suffix == ".ps1":
            return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", resolved]
        return [resolved]

    def _complete(self, system: str, user: str, temperature: float) -> LLMResponse:
        prompt = f"{system}\n\n{user}"
        argv = self._launcher()
        stdin_text: str | None = None

        if len(prompt) > self.MAX_ARG_CHARS:
            # Too long for a command line: send it on stdin and drop the flag.
            logger.info("Prompt is %s chars; passing it on stdin.", len(prompt))
            stdin_text = prompt
            argv += [part for part in self.command[1:] if "{prompt}" not in part and part != "-p"]
        else:
            argv += [part.replace("{prompt}", prompt) for part in self.command[1:]]

        try:
            proc = subprocess.run(
                # Always feed stdin so an unexpected interactive prompt hits EOF
                # instead of hanging the pipeline.
                argv, input=stdin_text or "", capture_output=True, text=True,
                timeout=self.timeout, check=False, encoding="utf-8", errors="replace",
            )
        except subprocess.TimeoutExpired as exc:
            raise LLMError(f"{self.name} timed out after {self.timeout}s") from exc
        except OSError as exc:
            raise LLMError(f"{self.name} could not be launched: {exc}") from exc

        combined = f"{proc.stdout}\n{proc.stderr}".lower()
        if any(marker in combined for marker in self._STUB_MARKERS):
            self._broken = True
            raise LLMUnavailable(
                "The 'copilot' command on PATH is an installer stub, not the Copilot CLI. "
                "Install it (npm i -g @github/copilot) and run 'copilot' once to sign in, "
                "or set an API key instead."
            )
        if proc.returncode != 0:
            self._broken = True
            raise LLMError(f"{self.name} exited {proc.returncode}: {(proc.stderr or '').strip()[:400]}")
        return LLMResponse(text=proc.stdout, provider=self.name, model=self.model or self.command[0])


class OpenAIProvider(LLMProvider):
    name = "openai"

    def available(self) -> bool:
        if not _api_key("OPENAI_API_KEY"):
            return False
        try:
            import openai  # noqa: F401
        except ImportError:
            return False
        return True

    def _complete(self, system: str, user: str, temperature: float) -> LLMResponse:
        from openai import OpenAI

        client = OpenAI(api_key=_api_key("OPENAI_API_KEY"))
        model = self.model or "gpt-4o"
        result = client.chat.completions.create(
            model=model,
            temperature=temperature,
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        )
        usage = getattr(result, "usage", None)
        return LLMResponse(
            text=result.choices[0].message.content or "",
            provider=self.name, model=model,
            tokens_in=getattr(usage, "prompt_tokens", None),
            tokens_out=getattr(usage, "completion_tokens", None),
        )


class AnthropicProvider(LLMProvider):
    name = "anthropic"

    def available(self) -> bool:
        if not _api_key("ANTHROPIC_API_KEY"):
            return False
        try:
            import anthropic  # noqa: F401
        except ImportError:
            return False
        return True

    def _complete(self, system: str, user: str, temperature: float) -> LLMResponse:
        import anthropic

        client = anthropic.Anthropic(api_key=_api_key("ANTHROPIC_API_KEY"))
        model = self.model or "claude-sonnet-4-20250514"
        result = client.messages.create(
            model=model,
            max_tokens=int(self.options.get("max_tokens", 8000)),
            temperature=temperature,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        text = "".join(block.text for block in result.content if getattr(block, "type", "") == "text")
        return LLMResponse(
            text=text, provider=self.name, model=model,
            tokens_in=result.usage.input_tokens, tokens_out=result.usage.output_tokens,
        )


class GeminiProvider(LLMProvider):
    """Vertex AI / Gemini. Reuses the same ADC the pipeline already needs."""

    name = "gemini"

    def available(self) -> bool:
        try:
            import google.generativeai  # noqa: F401
        except ImportError:
            return False
        return bool(
            _api_key("GOOGLE_API_KEY")
            or os.environ.get("GOOGLE_CLOUD_PROJECT")
        )

    def _complete(self, system: str, user: str, temperature: float) -> LLMResponse:
        import google.generativeai as genai

        if key := _api_key("GOOGLE_API_KEY"):
            genai.configure(api_key=key)
        model_name = self.model or "gemini-2.0-flash"
        model = genai.GenerativeModel(model_name, system_instruction=system)
        result = model.generate_content(
            user,
            generation_config={"temperature": temperature, "response_mime_type": "application/json"},
        )
        return LLMResponse(text=result.text or "", provider=self.name, model=model_name)


class LiteLLMProvider(LLMProvider):
    """LiteLLM adapter for OpenAI-compatible and other routed endpoints."""

    name = "litellm"

    def __init__(
        self,
        model: str = "",
        timeout: int = 180,
        api_key: str | None = None,
        api_base: str | None = None,
        **options: Any,
    ) -> None:
        super().__init__(model, timeout, **options)
        self._api_key_override = api_key
        self.api_base = api_base

    def _resolved_api_key(self) -> str | None:
        return (
            self._api_key_override
            or _api_key("LITELLM_API_KEY")
            or _api_key("OPENAI_API_KEY")
        )

    def _resolved_api_base(self) -> str | None:
        return (
            self.api_base
            or os.environ.get("LLM_API_BASE")
            or os.environ.get("OPENAI_API_BASE")
        )

    def available(self) -> bool:
        if not self.model or not self._resolved_api_key():
            return False
        try:
            import litellm  # noqa: F401
        except ImportError:
            return False
        return True

    def _complete(self, system: str, user: str, temperature: float) -> LLMResponse:
        try:
            import litellm
        except ImportError:
            raise LLMUnavailable(
                "LiteLLM is not installed; install the framework requirements."
            ) from None

        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "timeout": self.timeout,
        }
        api_key = self._resolved_api_key()
        api_base = self._resolved_api_base()
        if api_key:
            kwargs["api_key"] = api_key
        if api_base:
            kwargs["api_base"] = api_base

        try:
            result = litellm.completion(**kwargs)
        except Exception as exc:
            status_code = getattr(exc, "status_code", None)
            raise LLMRequestError(
                type(exc).__name__,
                status_code if isinstance(status_code, int) else None,
            ) from None

        try:
            text = result.choices[0].message.content
        except (AttributeError, IndexError, TypeError):
            raise LLMError("LiteLLM returned an invalid response shape.") from None
        if not isinstance(text, str) or not text.strip():
            raise LLMError("LiteLLM returned no textual completion.")

        usage = getattr(result, "usage", None)
        return LLMResponse(
            text=text,
            provider=self.name,
            model=self.model,
            tokens_in=getattr(usage, "prompt_tokens", None),
            tokens_out=getattr(usage, "completion_tokens", None),
        )


class EchoProvider(LLMProvider):
    """Test double: returns queued canned responses."""

    name = "echo"

    def __init__(self, responses: list[str] | None = None, **options: Any) -> None:
        super().__init__(model="echo", **options)
        self.responses = list(responses or [])
        self.calls: list[tuple[str, str]] = []

    def available(self) -> bool:
        return True

    def _complete(self, system: str, user: str, temperature: float) -> LLMResponse:
        self.calls.append((system, user))
        text = self.responses.pop(0) if self.responses else "{}"
        return LLMResponse(text=text, provider=self.name, model="echo")


_REGISTRY: dict[str, type[LLMProvider]] = {
    "none": NullProvider,
    "copilot_cli": CopilotCliProvider,
    "openai": OpenAIProvider,
    "anthropic": AnthropicProvider,
    "gemini": GeminiProvider,
    "litellm": LiteLLMProvider,
    "echo": EchoProvider,
}

# Tried in order when provider is "auto".
_AUTO_ORDER = ["anthropic", "openai", "gemini", "copilot_cli"]


def get_provider(name: str = "auto", model: str = "", **options: Any) -> LLMProvider:
    """Resolve a provider by name, or auto-detect the first available one."""
    model = model or os.environ.get("LLM_MODEL", "")
    if name == "auto":
        name = os.environ.get("LLM_PROVIDER", "auto").strip() or "auto"
    if name == "auto":
        for candidate in _AUTO_ORDER:
            provider = _REGISTRY[candidate](model=model, **options)
            if provider.available():
                logger.info("Auto-selected LLM provider %r", candidate)
                return provider
        logger.warning("No LLM provider available; falling back to rule-based extraction.")
        return NullProvider()

    if name not in _REGISTRY:
        raise LLMError(f"Unknown LLM provider {name!r}. Known: {sorted(_REGISTRY)}")
    return _REGISTRY[name](model=model, **options)


def _api_key(provider_variable: str) -> str | None:
    """Resolve a vendor key first, then the framework's generic local key."""
    return os.environ.get(provider_variable) or os.environ.get("LLM_API_KEY")


def extract_json(text: str) -> dict[str, Any]:
    """Pull the first JSON object out of a model response.

    Models wrap JSON in prose or fences even when told not to, so this is
    deliberately forgiving.
    """
    if not text or not text.strip():
        raise LLMError("LLM returned an empty response.")

    candidate = text.strip()
    if "```" in candidate:
        blocks = candidate.split("```")
        for block in blocks[1:]:
            body = block[4:] if block.lower().startswith("json") else block
            body = body.strip()
            if body.startswith("{"):
                candidate = body
                break

    start, depth, in_string, escape = candidate.find("{"), 0, False, False
    if start == -1:
        raise LLMError(f"No JSON object found in LLM response: {text[:200]!r}")

    for index in range(start, len(candidate)):
        char = candidate[index]
        if escape:
            escape = False
        elif char == "\\":
            escape = True
        elif char == '"':
            in_string = not in_string
        elif not in_string:
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(candidate[start : index + 1])
                    except json.JSONDecodeError as exc:
                        raise LLMError(f"LLM returned malformed JSON: {exc}") from exc
    raise LLMError("LLM response contained an unterminated JSON object.")


__all__ = [
    "LLMProvider", "LLMResponse", "LLMError", "LLMUnavailable",
    "NullProvider", "CopilotCliProvider", "OpenAIProvider", "AnthropicProvider",
    "GeminiProvider", "LiteLLMProvider", "EchoProvider", "get_provider", "extract_json",
]
