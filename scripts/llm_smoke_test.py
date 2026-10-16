"""Smoke-test the configured LLM provider end to end.

Run:  python scripts/llm_smoke_test.py [provider]

Confirms the provider launches, returns text, and that the text parses as the
JSON the agents expect. Makes no GCS or BigQuery calls.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.llm.provider import extract_json, get_provider  # noqa: E402
from src.utils.logging_setup import setup_logging  # noqa: E402


def main() -> int:
    setup_logging("INFO")
    name = sys.argv[1] if len(sys.argv) > 1 else "auto"
    provider = get_provider(name)
    print(f"provider   : {provider.name}")
    print(f"available  : {provider.available()}")
    if not provider.available():
        print("\nNo provider is reachable. The pipeline will use rule-based extraction.")
        return 1

    response = provider.complete(
        "You return exactly one JSON object and no other text.",
        'Return this object verbatim: {"answer": "county", "confidence": 0.9, "rationale": "smoke test"}',
    )
    print(f"latency    : {response.latency_ms} ms")
    print(f"raw        : {response.text[:500]!r}")
    try:
        print(f"parsed     : {extract_json(response.text)}")
    except Exception as exc:  # noqa: BLE001 - surface whatever went wrong
        print(f"PARSE FAILED: {exc}")
        return 1
    print("\nOK - this provider can drive the extraction agent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
