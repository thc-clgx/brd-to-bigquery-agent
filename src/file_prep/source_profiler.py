"""Profile the physical layout of source CSVs already staged in GCS.

The BRD describes the *contract* (column names, types, required metadata). The
files that actually land in the bucket do not always match it: headers may be
snake_case instead of camelCase, ``run_date``/``Source_FileName`` may be absent,
leading zeros may have been stripped, and there may be extra columns.

Profiling reads the real header so the 00 external table mirrors the file
byte-for-byte, and the 00 -> 01 load carries the burden of normalising back to
the BRD contract. Without this the 00 table would map declared names onto
source columns *positionally* and silently corrupt the data.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from typing import Any

from ..models.pipeline_spec import TableSpec
from ..utils.logging_setup import get_logger

logger = get_logger(__name__)

# Widths for zero-padded code columns whose leading zeros the source may have lost.
_ZERO_PAD_WIDTHS_RAW = {
    "countyFIPSCode": 5,
    "stateCode": 2,
    "CntyCd": 3,
    "GeoAddressPostcode": 5,
}


# Abbreviations are folded to one form so ``netCountyPctChg`` matches
# ``net_county_pct_change``.
_ABBREVIATIONS = [
    ("change", "chg"),
    ("percentage", "pct"),
    ("percent", "pct"),
    ("number", "nbr"),
    ("identifier", "id"),
    ("county", "cnty"),
]


def _norm(header: str) -> str:
    """Collapse a header to a comparison key: ``county_fips_code`` -> ``cntyfipscode``."""
    key = "".join(ch for ch in header.lower() if ch.isalnum())
    for long_form, short_form in _ABBREVIATIONS:
        key = key.replace(long_form, short_form)
    return key


_ZERO_PAD_WIDTHS = {_norm(name): width for name, width in _ZERO_PAD_WIDTHS_RAW.items()}


@dataclass
class SourceProfile:
    """The physical layout of one staged object, mapped onto the BRD schema."""

    object_name: str
    headers: list[str]
    # BRD column name -> physical header in the file.
    mapping: dict[str, str] = field(default_factory=dict)
    # BRD columns with no physical counterpart (supplied as literals at load).
    missing: list[str] = field(default_factory=list)
    # Physical columns the BRD does not describe (landed in 00, dropped at 01).
    extra: list[str] = field(default_factory=list)

    @property
    def conformant(self) -> bool:
        return not self.missing and not self.extra

    def to_dict(self) -> dict[str, Any]:
        return {
            "object_name": self.object_name,
            "headers": self.headers,
            "mapping": self.mapping,
            "missing": self.missing,
            "extra": self.extra,
            "conformant": self.conformant,
        }


def zero_pad_width(column_name: str) -> int | None:
    return _ZERO_PAD_WIDTHS.get(_norm(column_name))


def profile_headers(table: TableSpec, headers: list[str], object_name: str = "") -> SourceProfile:
    """Match a physical header row onto the table's BRD columns."""
    headers = [h.strip().lstrip("\ufeff") for h in headers]
    by_key = {_norm(h): h for h in headers}

    mapping: dict[str, str] = {}
    missing: list[str] = []
    for column in table.columns_00:
        physical = by_key.get(_norm(column.name))
        if physical is None and column.source_header:
            physical = by_key.get(_norm(column.source_header))
        if physical is None:
            missing.append(column.name)
        else:
            mapping[column.name] = physical

    claimed = set(mapping.values())
    extra = [h for h in headers if h not in claimed]

    profile = SourceProfile(object_name, headers, mapping, missing, extra)
    if not profile.conformant:
        logger.warning(
            "%s: source layout differs from the BRD - missing %s, extra %s",
            table.source_name, missing or "none", extra or "none",
        )
    return profile


def profile_gcs_object(client, bucket: str, object_name: str, table: TableSpec) -> SourceProfile:
    """Read just the header row of a GCS CSV and profile it."""
    blob = client.bucket(bucket).blob(object_name)
    # Header rows are tiny; never pull a multi-MB object to read one line.
    raw = blob.download_as_bytes(start=0, end=8191).decode("utf-8-sig", errors="replace")
    first_line = raw.splitlines()[0] if raw.splitlines() else ""
    headers = next(csv.reader(io.StringIO(first_line)), [])
    return profile_headers(table, headers, object_name)


def profile_table(client, bucket: str, table: TableSpec, prefix: str | None = None) -> SourceProfile | None:
    """Profile the first CSV found under the table's GCS folder."""
    prefix = prefix if prefix is not None else table.gcs_prefix()
    names = [
        b.name for b in client.list_blobs(bucket, prefix=prefix)
        if b.name.lower().endswith(".csv")
    ]
    if not names:
        logger.warning("%s: no CSV found under gs://%s/%s", table.source_name, bucket, prefix)
        return None
    return profile_gcs_object(client, bucket, sorted(names)[0], table)


__all__ = ["SourceProfile", "profile_headers", "profile_gcs_object", "profile_table", "zero_pad_width"]
