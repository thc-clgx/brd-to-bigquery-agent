from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from src.orchestrator.executer import (
    LauncherError, find_brd, load_environment_file, run_from_environment,
)


def test_env_loader_reads_external_input_path_and_quoted_values_without_execution(tmp_path):
    config = tmp_path / "private.env"
    config.write_text(
        '# private settings\nexport BRD_INPUT_DIR="/tmp/BRD inputs"\n'
        "LLM_MODEL='model with spaces'\nLLM_API_KEY='test-secret-value'\n",
        encoding="utf-8",
    )
    values = load_environment_file(config)
    assert values["BRD_INPUT_DIR"] == "/tmp/BRD inputs"
    assert values["LLM_MODEL"] == "model with spaces"
    assert values["LLM_API_KEY"] == "test-secret-value"


def test_env_loader_rejects_shell_substitution_without_executing(tmp_path):
    marker = tmp_path / "should-not-exist"
    config = tmp_path / "unsafe.env"
    config.write_text(f"BRD_INPUT_DIR=$(touch {marker})\n", encoding="utf-8")
    assert load_environment_file(config)["BRD_INPUT_DIR"].startswith("$(touch")
    assert not marker.exists()


def test_input_folder_requires_exactly_one_brd_and_ignores_source_data(tmp_path):
    (tmp_path / "requirements.docx").touch()
    (tmp_path / "~$requirements.docx").touch()
    (tmp_path / "daily.csv").touch()
    assert find_brd(tmp_path).name == "requirements.docx"
    (tmp_path / "other.pdf").touch()
    with pytest.raises(LauncherError, match="found 2"):
        find_brd(tmp_path)


@pytest.mark.parametrize("config_text", [
    "BRD_INPUT_DIR=/tmp\nINVALID LINE\n",
    'BRD_INPUT_DIR="/tmp\n',
    "9INVALID=/tmp\n",
])
def test_invalid_env_syntax_is_actionable(tmp_path, config_text):
    config = tmp_path / "bad.env"
    config.write_text(config_text, encoding="utf-8")
    with pytest.raises(LauncherError):
        load_environment_file(config)


def test_launcher_uses_external_folder_and_dispatches_by_brd_contract(tmp_path):
    inputs = tmp_path / "my-data"
    inputs.mkdir()
    brd = inputs / "spec.txt"
    brd.write_text("ordinary project contract", encoding="utf-8")
    config = tmp_path / "private.env"
    config.write_text(
        f'BRD_INPUT_DIR="{inputs}"\nLLM_PROVIDER=litellm\nLLM_MODEL=test-model\n'
        "LLM_API_KEY=secret\nGOOGLE_CLOUD_PROJECT=test-project-123\n",
        encoding="utf-8",
    )
    runner = Mock(return_value={"status": "DRY_RUN_READY", "run_id": "test-run"})
    report = run_from_environment(
        env_file=config, environ={}, dry_run=True, generic_runner=runner,
    )
    assert report["status"] == "DRY_RUN_READY"
    args, kwargs = runner.call_args
    assert args == (brd, inputs.resolve(), args[2])
    assert kwargs == {"dry_run": True, "verify_retry": False}
    assert "secret" not in repr(report)


def test_launcher_writes_correction_report_for_missing_brd_input(tmp_path):
    config = tmp_path / "private.env"
    config.write_text("LLM_PROVIDER=litellm\nLLM_MODEL=test\n", encoding="utf-8")
    report = run_from_environment(env_file=config, environ={})
    assert report["status"] == "BLOCKED"
    path = Path(report["report_path"])
    text = path.read_text(encoding="utf-8")
    assert "BRD_INPUT_DIR" in text
    assert "test" not in text
