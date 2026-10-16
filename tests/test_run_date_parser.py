from datetime import date

import pytest

from src.utils.run_date import (
    RunDateParseError,
    find_run_identifiers,
    normalize_run_id,
    parse_run_date,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2025_04", date(2025, 4, 1)),
        ("202504", date(2025, 4, 1)),
        ("2025-04", date(2025, 4, 1)),
        ("2025/04", date(2025, 4, 1)),
        ("2026_04", date(2026, 4, 1)),
        ("202510", date(2025, 10, 1)),
        ("  2025_04  ", date(2025, 4, 1)),
    ],
)
def test_year_month_becomes_first_of_month(raw, expected):
    assert parse_run_date(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [("2025-04-15", date(2025, 4, 15)), ("20250401", date(2025, 4, 1))],
)
def test_full_dates_are_preserved(raw, expected):
    assert parse_run_date(raw) == expected


def test_date_passes_through():
    assert parse_run_date(date(2025, 4, 1)) == date(2025, 4, 1)


@pytest.mark.parametrize("raw", ["", "   ", "2025_13", "abcd", "25_04", "2025_00", None])
def test_invalid_identifiers_raise(raw):
    with pytest.raises(RunDateParseError):
        parse_run_date(raw)


@pytest.mark.parametrize("raw", ["202504", "2025-04", "2025_04"])
def test_normalize_run_id(raw):
    assert normalize_run_id(raw) == "2025_04"


def test_find_run_identifiers_in_text():
    text = "Historical Load:\n2025_04\n2026_04\nTarget Date: 30-Sep-2026"
    assert find_run_identifiers(text) == ["2025_04", "2026_04"]
