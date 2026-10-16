"""Run identifier parsing.

A "run identifier" is how a surveillance run is named in the source GCS path
(``2025_04``, ``202504``, ...). BigQuery stores it as a DATE pinned to the first
of the month (``2025-04-01``) so the 01 table can be partitioned by ``run_date``.
"""

from __future__ import annotations

import re
from datetime import date

# 202504 | 2025_04 | 2025-04 | 2025/04 | 2025.04 | 2025 04
_YEAR_MONTH = re.compile(r"^(?P<year>(?:19|20)\d{2})[ _\-/.]?(?P<month>0[1-9]|1[0-2])$")
# 2025-04-01 | 2025_04_01 | 20250401
_YEAR_MONTH_DAY = re.compile(
    r"^(?P<year>(?:19|20)\d{2})[ _\-/.]?(?P<month>0[1-9]|1[0-2])[ _\-/.]?(?P<day>0[1-9]|[12]\d|3[01])$"
)


class RunDateParseError(ValueError):
    """Raised when a run identifier cannot be interpreted."""


def parse_run_date(run_identifier: str | date) -> date:
    """Convert a run identifier into the DATE stored in ``run_date``.

    ``2025_04`` / ``202504`` / ``2025-04`` -> ``date(2025, 4, 1)``
    A full date is honoured as-is (``2025-04-15`` -> ``date(2025, 4, 15)``).
    """
    if isinstance(run_identifier, date):
        return run_identifier
    if run_identifier is None:
        raise RunDateParseError("Run identifier is required but was None.")

    token = str(run_identifier).strip()
    if not token:
        raise RunDateParseError("Run identifier is required but was empty.")

    match = _YEAR_MONTH_DAY.match(token)
    if match:
        return date(int(match["year"]), int(match["month"]), int(match["day"]))

    match = _YEAR_MONTH.match(token)
    if match:
        return date(int(match["year"]), int(match["month"]), 1)

    raise RunDateParseError(
        f"Cannot parse run identifier {token!r}. Expected YYYYMM, YYYY_MM, YYYY-MM or a full YYYY-MM-DD date."
    )


def normalize_run_id(run_identifier: str | date) -> str:
    """Canonical ``YYYY_MM`` form used in file names and GCS object keys."""
    run_date = parse_run_date(run_identifier)
    return f"{run_date.year:04d}_{run_date.month:02d}"


def find_run_identifiers(text: str) -> list[str]:
    """Extract candidate run identifiers (``YYYY_MM``) from free text, in order."""
    found: list[str] = []
    for token in re.findall(r"\b((?:19|20)\d{2}[_\-/.]?(?:0[1-9]|1[0-2]))\b", text):
        try:
            normalized = normalize_run_id(token)
        except RunDateParseError:
            continue
        if normalized not in found:
            found.append(normalized)
    return found
