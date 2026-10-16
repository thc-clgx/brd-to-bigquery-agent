"""Single-command launcher using an external input folder and private env file."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from ..utils.config import PROJECT_ROOT

DEFAULT_ENV_FILES = (
    PROJECT_ROOT / ".env",
    Path.home() / ".config" / "brd-to-bigquery-agent" / "rainfall-test.env",
)
BRD_EXTENSIONS = {".docx", ".pdf", ".txt", ".md", ".markdown"}


class LauncherError(ValueError):
    """Safe, actionable launcher configuration/input error."""


def load_environment_file(path: Path) -> dict[str, str]:
    """Read simple KEY=VALUE lines without executing shell code or printing values."""
    if not path.is_file():
        raise LauncherError(f"Private environment file not found: {path}")
    result: dict[str, str] = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[7:].lstrip()
        if "=" not in stripped:
            raise LauncherError(f"Invalid environment-file syntax at line {line_number}.")
        name, raw_value = stripped.split("=", 1)
        name = name.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise LauncherError(f"Invalid environment variable name at line {line_number}.")
        value = raw_value.strip()
        if value.startswith(("'", '"')):
            quote = value[0]
            if len(value) < 2 or value[-1] != quote:
                raise LauncherError(f"Unclosed quoted value at environment-file line {line_number}.")
            value = value[1:-1]
        else:
            value = value.split("#", 1)[0].rstrip()
        result[name] = value
    return result


def find_brd(input_root: Path) -> Path:
    if not input_root.is_dir():
        raise LauncherError("BRD_INPUT_DIR must point to an existing input folder.")
    documents = sorted(
        path for path in input_root.rglob("*")
        if path.is_file() and not path.name.startswith("~$")
        and path.suffix.lower() in BRD_EXTENSIONS
    )
    if len(documents) != 1:
        raise LauncherError(
            f"Expected exactly one BRD document under BRD_INPUT_DIR; found {len(documents)}. "
            "Keep source data as CSV/Excel and remove or separate other document files."
        )
    return documents[0]


def write_launcher_report(output_root: Path, *, status: str, check: str, detail: str, action: str) -> Path:
    directory = output_root / "reports"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"executer_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}.md"
    document = [
        "# Framework launcher report", "", f"**Status: {status}**", "",
        "| Check | Result | Action needed |", "| --- | --- | --- |",
        f"| {check} | {detail.replace('|', '/')} | {action.replace('|', '/')} |", "",
    ]
    path.write_text("\n".join(document), encoding="utf-8")
    return path


def run_from_environment(
    *,
    env_file: Path | None = None,
    environ: dict[str, str] | None = None,
    dry_run: bool = False,
    verify_idempotency: bool = False,
    rainfall_runner: Callable | None = None,
    generic_runner: Callable | None = None,
) -> dict:
    configured_env = env_file or os.environ.get("BRD_AGENT_ENV_FILE")
    path = Path(configured_env).expanduser() if configured_env else next(
        (candidate for candidate in DEFAULT_ENV_FILES if candidate.is_file()),
        DEFAULT_ENV_FILES[0],
    )
    output_root = PROJECT_ROOT / "generated"
    try:
        file_values = load_environment_file(path)
        effective = dict(os.environ if environ is None else environ)
        for key, value in file_values.items():
            effective.setdefault(key, value)
        input_value = effective.get("BRD_INPUT_DIR", "").strip()
        if not input_value or input_value.startswith("<"):
            raise LauncherError("BRD_INPUT_DIR is missing from the private environment configuration.")
        input_root = Path(input_value).expanduser().resolve()
        brd_path = find_brd(input_root)
        if not effective.get("LLM_PROVIDER") or not effective.get("LLM_MODEL"):
            raise LauncherError("Configure LLM_PROVIDER and LLM_MODEL in the private environment file.")
        if not effective.get("GOOGLE_CLOUD_PROJECT"):
            raise LauncherError("Set GOOGLE_CLOUD_PROJECT to the approved project from the BRD.")
    except (LauncherError, OSError) as error:
        report_path = write_launcher_report(
            output_root, status="BLOCKED", check="input configuration",
            detail=str(error), action=(
                "Set BRD_INPUT_DIR to the external folder containing exactly one supported BRD "
                "and its source files; verify the private env-file path."
            ),
        )
        return {"status": "BLOCKED", "report_path": str(report_path), "error": str(error)}

    # Only populate absent process variables; do not overwrite credentials/config explicitly set by caller.
    if environ is None:
        for key, value in file_values.items():
            os.environ.setdefault(key, value)
    from ..brd_parser.document_reader import read_document
    from .managed_runner import execute_generic, execute_rainfall

    try:
        text = read_document(brd_path).text
    except (OSError, ValueError) as error:
        report_path = write_launcher_report(
            output_root, status="BLOCKED", check="BRD reading",
            detail=f"{type(error).__name__}; document contents omitted.",
            action="Provide a readable DOCX, text-based PDF, TXT, or Markdown BRD.",
        )
        return {"status": "BLOCKED", "report_path": str(report_path), "error": "BRD could not be read."}

    rainfall = all(name in text for name in (
        "01_dim_station_watershed", "01_fact_daily_station_rainfall",
        "01_fact_watershed_station_coverage", "01_fact_station_rainfall_assessment",
    ))
    runner = (rainfall_runner if rainfall else generic_runner) or (
        execute_rainfall if rainfall else execute_generic
    )
    report = runner(
        brd_path, input_root, output_root, dry_run=dry_run,
        verify_retry=verify_idempotency,
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the BRD-driven workflow using BRD_INPUT_DIR from the private env file."
    )
    parser.add_argument("--env-file", type=Path, default=None,
                        help="Private KEY=VALUE file (default: project .env, then legacy user settings)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate and plan without cloud mutations; LLM and read-only cloud checks still run.")
    parser.add_argument("--verify-idempotency", action="store_true",
                        help="After execution, replay the transformation and require zero target changes.")
    args = parser.parse_args(argv)
    report = run_from_environment(
        env_file=args.env_file, dry_run=args.dry_run, verify_idempotency=args.verify_idempotency,
    )
    print(json.dumps({
        "status": report.get("status"),
        "run_id": report.get("run_id"),
        "report_path": report.get("report_path") or report.get("report_directory"),
        "issues": report.get("issues"),
        "error": report.get("error"),
    }, indent=2))
    return 0 if report.get("status") in {"SUCCESS", "DRY_RUN_READY"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
