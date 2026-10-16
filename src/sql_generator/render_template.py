"""Thin Jinja2 wrapper plus the helper that writes SQL to ``generated/sql``."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from ..utils.config import PROJECT_ROOT
from ..utils.logging_setup import get_logger

logger = get_logger(__name__)

DEFAULT_TEMPLATES_DIR = PROJECT_ROOT / "templates"


class SqlRenderer:
    """Renders the ``.sql.j2`` templates with undefined-variable checking on."""

    def __init__(self, templates_dir: str | Path | None = None) -> None:
        self.templates_dir = Path(templates_dir) if templates_dir else DEFAULT_TEMPLATES_DIR
        if not self.templates_dir.is_absolute():
            self.templates_dir = PROJECT_ROOT / self.templates_dir
        if not self.templates_dir.exists():
            raise FileNotFoundError(f"Templates directory not found: {self.templates_dir}")
        self.env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            undefined=StrictUndefined,
            trim_blocks=False,
            lstrip_blocks=False,
            keep_trailing_newline=True,
        )
        self.env.filters["sql_string"] = sql_string

    def render(self, template_name: str, context: dict) -> str:
        sql = self.env.get_template(template_name).render(**context)
        return _tidy(sql)


def sql_string(value) -> str:
    """Escape a value for use inside a double-quoted BigQuery string literal."""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def _tidy(sql: str) -> str:
    """Collapse the blank lines Jinja leaves behind so generated SQL stays readable."""
    lines = [line.rstrip() for line in sql.splitlines()]
    tidied: list[str] = []
    for line in lines:
        if not line and tidied and not tidied[-1]:
            continue
        tidied.append(line)
    return "\n".join(tidied).strip() + "\n"


def write_sql(sql: str, output_dir: str | Path, file_name: str) -> Path:
    """Persist rendered SQL and return the path."""
    directory = Path(output_dir)
    if not directory.is_absolute():
        directory = PROJECT_ROOT / directory
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / file_name
    path.write_text(sql, encoding="utf-8")
    logger.info("Wrote SQL -> %s", path)
    return path
