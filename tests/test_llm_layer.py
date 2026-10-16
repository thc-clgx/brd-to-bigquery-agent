"""Tests for the LLM and agentic decision layer.

Every test runs offline against a scripted provider. Nothing here contacts a
real model, so the suite stays fast and deterministic.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from src.brd_parser.document_reader import BrdDocument, BrdLine
from src.brd_parser.hybrid_extractor import HybridExtractor
from src.llm.decision_agent import DecisionAgent
from src.llm.extraction_agent import LLMExtractionAgent
from src.llm.provider import (
    EchoProvider,
    LLMError,
    LiteLLMProvider,
    NullProvider,
    extract_json,
    get_provider,
)


def _document() -> BrdDocument:
    lines = [
        BrdLine(index=0, text="Wildfire surveillance refresh", level=0),
        BrdLine(index=1, text="Load the brushfire change logs each month.", level=1),
    ]
    return BrdDocument(path=Path("memo.docx"), lines=lines)


def _payload(**overrides):
    payload = {
        "dataset_00": "ampd_00_hazard_wildfire_test",
        "dataset_01": "ampd_01_hazard_wildfire_test",
        "historical_runs": ["2025_04"],
        "directives": {"partition_by": "run_date", "cluster_candidates": ["countyFIPSCode"]},
        "tables": [
            {
                "source_name": "Brushfire",
                "table_00": "00_wfrs_brushfire_changelogs_test",
                "table_01": "01_wfrs_brushfire_changelogs_county_test",
                "grain": "county",
                "columns": [
                    {"name": "countyFIPSCode", "type": "STRING", "preserve_leading_zeros": True},
                    {"name": "riskValue", "type": "INTEGER"},
                ],
            }
        ],
        "assumptions": [],
        "unsupported": [],
    }
    payload.update(overrides)
    return json.dumps(payload)


# -- JSON extraction --------------------------------------------------------
def test_extract_json_handles_fenced_and_chatty_responses():
    assert extract_json('```json\n{"a": 1}\n```') == {"a": 1}
    assert extract_json('Sure! Here you go:\n{"a": {"b": 2}}\nHope that helps.') == {"a": {"b": 2}}


def test_extract_json_ignores_braces_inside_strings():
    assert extract_json('{"sql": "SELECT {not json}"}') == {"sql": "SELECT {not json}"}


def test_extract_json_rejects_unusable_responses():
    for bad in ["", "no json here", '{"a": ']:
        with pytest.raises(LLMError):
            extract_json(bad)


# -- providers --------------------------------------------------------------
def test_null_provider_is_never_available():
    provider = NullProvider()
    assert provider.available() is False
    with pytest.raises(LLMError):
        provider.complete("system", "user")


def test_explicit_llm_extraction_never_silently_falls_back_to_rules(legacy_config):
    with pytest.raises(LLMError, match="refusing a rules-only fallback"):
        HybridExtractor(legacy_config, mode="llm", provider=NullProvider())


def test_unknown_provider_is_rejected():
    with pytest.raises(LLMError):
        get_provider("definitely-not-a-provider")


def test_litellm_provider_uses_environment_credentials_and_openai_compatible_api(monkeypatch, caplog):
    recorded = {}

    def fake_completion(**kwargs):
        recorded.update(kwargs)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content='{"summary":"ok"}'))],
            usage=SimpleNamespace(prompt_tokens=12, completion_tokens=4),
        )

    fake_litellm = ModuleType("litellm")
    fake_litellm.completion = fake_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_litellm)
    secret = "unit-test-secret-never-logged"
    monkeypatch.setenv("LLM_API_KEY", secret)
    monkeypatch.setenv("LLM_API_BASE", "https://llm.example.test/v1")

    provider = get_provider("litellm", "openai/test-deployment")
    response = provider.complete("system prompt", "user prompt", temperature=0.0)

    assert recorded["model"] == "openai/test-deployment"
    assert recorded["api_key"] == secret
    assert recorded["api_base"] == "https://llm.example.test/v1"
    assert recorded["messages"] == [
        {"role": "system", "content": "system prompt"},
        {"role": "user", "content": "user prompt"},
    ]
    assert response.text == '{"summary":"ok"}'
    assert response.provider == "litellm"
    assert response.tokens_in == 12
    assert response.tokens_out == 4
    assert secret not in caplog.text
    assert secret not in repr(response.to_dict())


def test_litellm_provider_redacts_upstream_error_details(monkeypatch):
    secret = "unit-test-secret-never-logged"

    def failing_completion(**_kwargs):
        raise RuntimeError(f"upstream error included credential {secret}")

    fake_litellm = ModuleType("litellm")
    fake_litellm.completion = failing_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_litellm)
    provider = LiteLLMProvider(
        "openai/test-deployment",
        api_key=secret,
        api_base="https://llm.example.test/v1",
    )

    with pytest.raises(LLMError) as exc_info:
        provider.complete("system prompt", "user prompt")

    assert secret not in str(exc_info.value)
    assert exc_info.value.__cause__ is None


def test_litellm_provider_is_unavailable_without_a_key(monkeypatch):
    fake_litellm = ModuleType("litellm")
    fake_litellm.completion = lambda **_kwargs: None
    monkeypatch.setitem(sys.modules, "litellm", fake_litellm)
    for name in ("LLM_API_KEY", "LITELLM_API_KEY", "OPENAI_API_KEY"):
        monkeypatch.delenv(name, raising=False)

    assert LiteLLMProvider("openai/test-deployment").available() is False


# -- extraction agent -------------------------------------------------------
def test_agent_builds_a_valid_spec_from_a_model_proposal(legacy_config):
    agent = LLMExtractionAgent(EchoProvider([_payload()]), legacy_config)
    result = agent.extract(_document(), run_id="2025_04")

    assert result.ok
    spec = result.spec
    assert spec.parser == "llm:echo"
    assert spec.dataset_00 == "ampd_00_hazard_wildfire_test"
    assert [t.table_01 for t in spec.tables] == ["01_wfrs_brushfire_changelogs_county_test"]


def test_agent_adds_audit_columns_the_model_omitted(legacy_config):
    agent = LLMExtractionAgent(EchoProvider([_payload()]), legacy_config)
    spec = agent.extract(_document(), run_id="2025_04").spec

    names = [c.name for c in spec.tables[0].schema_columns]
    assert {"run_date", "Source_FileName", "LOAD_DATE_01"} <= set(names)
    load_date = next(c for c in spec.tables[0].schema_columns if c.name == "LOAD_DATE_01")
    assert load_date.added_at_load is True


def test_agent_forces_string_for_zero_padded_codes(legacy_config):
    """A FIPS code typed INT64 by the model must be corrected, not accepted."""
    bad = json.loads(_payload())
    bad["tables"][0]["columns"][0] = {
        "name": "countyFIPSCode", "type": "INT64", "preserve_leading_zeros": True,
    }
    agent = LLMExtractionAgent(EchoProvider([json.dumps(bad)]), legacy_config)
    spec = agent.extract(_document(), run_id="2025_04").spec

    column = next(c for c in spec.tables[0].schema_columns if c.name == "countyFIPSCode")
    assert column.type == "STRING"


def test_agent_repairs_itself_after_a_failed_attempt(legacy_config):
    """The agentic loop: a bad proposal is critiqued and retried."""
    broken = json.dumps({"tables": [{"table_00": "brushfire", "table_01": "x", "columns": []}]})
    provider = EchoProvider([broken, _payload()])
    agent = LLMExtractionAgent(provider, legacy_config, max_attempts=3)

    result = agent.extract(_document(), run_id="2025_04")

    assert result.ok
    assert len(result.attempts) == 2
    assert result.attempts[0].ok is False
    # The second prompt must carry the critique, otherwise it is just a retry.
    assert "FAILED validation" in provider.calls[1][1]


def test_agent_gives_up_after_max_attempts(legacy_config):
    broken = json.dumps({"tables": []})
    agent = LLMExtractionAgent(EchoProvider([broken] * 5), legacy_config, max_attempts=2)

    result = agent.extract(_document(), run_id="2025_04")

    assert not result.ok
    assert len(result.attempts) == 2
    assert "did not converge" in result.error


def test_agent_never_lets_the_model_choose_the_project(legacy_config):
    """Config pins the project; a model suggestion must be ignored."""
    agent = LLMExtractionAgent(EchoProvider([_payload(project_id="some-other-project")]), legacy_config)
    spec = agent.extract(_document(), run_id="2025_04").spec
    assert spec.project_id == legacy_config.gcp.project_id


# -- decision agent ---------------------------------------------------------
def test_decision_agent_falls_back_when_disabled():
    agent = DecisionAgent(NullProvider(), enabled=False)
    decision = agent.decide("What is the grain?", {}, fallback="county")

    assert decision.answer == "county"
    assert decision.source == "rule"
    assert decision.needs_review is False


def test_low_confidence_answers_are_flagged_for_review():
    response = json.dumps({"answer": "state", "confidence": 0.3, "rationale": "unclear"})
    agent = DecisionAgent(EchoProvider([response]), threshold=0.7)

    decision = agent.decide("What is the grain?", {}, options=["county", "state"])

    assert decision.answer == "state"
    assert decision.needs_review is True
    assert agent.needs_review == [decision]


def test_answers_outside_the_allowed_options_are_rejected():
    response = json.dumps({"answer": "parcel", "confidence": 0.99, "rationale": "sure"})
    agent = DecisionAgent(EchoProvider([response]))

    decision = agent.decide("Grain?", {}, options=["county", "state"], fallback="county")

    assert decision.answer == "county"
    assert decision.source == "fallback"


def test_header_mapping_rejects_two_columns_bound_to_one_header():
    """A non-injective mapping would silently duplicate a column's data."""
    response = json.dumps({
        "mapping": {"acreageChange": "acr_chg", "netCountyPctChg": "acr_chg"},
        "confidence": 0.95,
    })
    agent = DecisionAgent(EchoProvider([response]))

    decision = agent.map_headers(
        "Brushfire",
        [{"name": "acreageChange"}, {"name": "netCountyPctChg"}],
        ["acr_chg"],
        fallback={},
    )

    assert decision.source == "fallback"
    assert decision.needs_review is True


def test_header_mapping_drops_hallucinated_headers():
    response = json.dumps({
        "mapping": {"acreageChange": "acr_chg", "riskValue": "a_column_that_does_not_exist"},
        "confidence": 0.9,
    })
    agent = DecisionAgent(EchoProvider([response]))

    decision = agent.map_headers("Brushfire", [{"name": "acreageChange"}], ["acr_chg"])

    assert decision.answer["mapping"] == {"acreageChange": "acr_chg"}


# -- hybrid routing ---------------------------------------------------------
def test_hybrid_downgrades_to_rules_when_llm_is_disabled(legacy_config):
    extractor = HybridExtractor(legacy_config, provider=NullProvider())
    assert extractor.mode == "rule_based"
    assert extractor.decision_agent.enabled is False


def test_explicit_rule_based_mode_never_calls_the_model(legacy_config):
    provider = EchoProvider([_payload()])
    extractor = HybridExtractor(legacy_config, mode="rule_based", provider=provider)
    assert extractor.mode == "rule_based"
    assert provider.calls == []


def test_llm_mode_produces_a_spec_the_validator_accepts(legacy_config):
    extractor = HybridExtractor(legacy_config, mode="llm", provider=EchoProvider([_payload()]))
    result = extractor.extract(_document(), run_id="2025_04")

    assert result.strategy == "llm"
    assert result.spec.tables[0].table_00 == "00_wfrs_brushfire_changelogs_test"
    # The rule parser cannot read this memo, which is exactly why the LLM ran.
    assert result.rule_error is not None


def test_llm_spec_generates_the_same_sql_the_rule_path_would(legacy_config):
    """The contract boundary holds: downstream code cannot tell who wrote the spec."""
    from src.sql_generator.generate_00_sql import generate_00_sql
    from src.sql_generator.generate_01_sql import generate_01_sql
    from src.sql_generator.generate_load_sql import generate_load_sql
    from src.sql_generator.render_template import SqlRenderer

    extractor = HybridExtractor(legacy_config, mode="llm", provider=EchoProvider([_payload()]))
    spec = extractor.extract(_document(), run_id="2025_04").spec
    table = spec.tables[0]
    renderer = SqlRenderer(legacy_config.paths.templates_dir)

    sql_00 = generate_00_sql(spec, table, legacy_config, renderer)
    sql_01 = generate_01_sql(spec, table, legacy_config, renderer)
    sql_load = generate_load_sql(spec, table, legacy_config, renderer)

    assert "CREATE OR REPLACE EXTERNAL TABLE" in sql_00
    # 00 is all-STRING regardless of what the model claimed the types were.
    assert "INT64" not in sql_00
    assert "PARTITION BY run_date" in sql_01
    assert "CLUSTER BY countyFIPSCode" in sql_01
    # Leading zeros are repaired at load time, as on the rule-based path.
    assert "LPAD(" in sql_load
    assert "SAFE_CAST" in sql_load


def test_hybrid_keeps_the_rule_spec_when_the_llm_cannot_converge(config, tmp_path):
    """A failing model must not destroy a usable rule-based result."""
    from src.brd_parser.document_reader import read_document

    brd = Path("input/WFRS_Surveillance_Detailed_Plan_20260903.docx")
    if not brd.exists():
        pytest.skip("real BRD not present")

    provider = EchoProvider(['{"tables": []}'] * 5)
    extractor = HybridExtractor(config, mode="hybrid", provider=provider)
    result = extractor.extract(read_document(brd), run_id="2025_04")

    # Rules succeed here, so the model is never consulted at all.
    assert result.strategy == "hybrid:rules"
    assert provider.calls == []
