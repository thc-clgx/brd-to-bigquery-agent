"""Converts source Excel change logs into load-ready CSVs.

Everything is read as text so leading zeros (``02013``, state/ZIP codes) survive
the round trip. The output header matches ``TableSpec.csv_columns`` exactly, which
is what the 00 external table declares.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ..models.pipeline_spec import PipelineSpec, TableSpec
from ..models.validation import ValidationReport
from ..utils.config import AppConfig, PROJECT_ROOT
from ..utils.file_access import cleanup_copy, local_readable_copy
from ..utils.logging_setup import get_logger

logger = get_logger(__name__)

# Header aliasing: BRD field names are camelCase, workbooks use prose headers.
_SYNONYMS = {
    "%": "pct", "percent": "pct", "percentage": "pct", "pct": "pct",
    "change": "chg", "chg": "chg", "changes": "chg",
    "cd": "code", "nbr": "number", "num": "number",
    "cnty": "county", "st": "state",
}
_STOPWORDS = {"in", "of", "the", "for", "a", "an", "at", "by"}

# Tokens that appear in every table name and therefore identify nothing.
_GENERIC_TABLE_TOKENS = {"wfrs", "chg", "changelog", "changelogs", "log", "logs", "00", "01", "table"}


@dataclass
class ConversionResult:
    source_name: str
    source_file: Path
    csv_path: Path | None
    row_count: int = 0
    columns: list[str] = field(default_factory=list)
    file_suffix: str | None = None
    skipped: bool = False
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "source_file": str(self.source_file),
            "csv_path": str(self.csv_path) if self.csv_path else None,
            "row_count": self.row_count,
            "columns": self.columns,
            "file_suffix": self.file_suffix,
            "skipped": self.skipped,
            "message": self.message,
        }


def normalize_header(text: str) -> str:
    """``'Net % Change in County'`` -> ``'netpctchgcounty'`` (order preserved)."""
    return "".join(_header_tokens(text))


def _header_tokens(text: str) -> list[str]:
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", str(text))
    spaced = spaced.replace("%", " % ")
    tokens = [t for t in re.split(r"[^A-Za-z0-9%]+", spaced.lower()) if t]
    result: list[str] = []
    for token in tokens:
        if token in _STOPWORDS:
            continue
        result.append(_SYNONYMS.get(token, token))
    return result


def _token_set(text: str) -> frozenset[str]:
    return frozenset(_header_tokens(text))


def _distinctive_tokens(table: TableSpec) -> set[str]:
    """Tokens that tell this table apart from its siblings, e.g. ``{'brushfire'}``."""
    return {
        token for token in _header_tokens(table.table_00)
        if len(token) >= 4 and token not in _GENERIC_TABLE_TOKENS
    }


class XlsxToCsvConverter:
    """Reads workbooks for one :class:`TableSpec` and writes CSVs for one run."""

    def __init__(self, config: AppConfig, dry_run: bool = True) -> None:
        self.config = config
        self.dry_run = dry_run

    # -- public API ---------------------------------------------------------
    def convert_table(
        self,
        spec: PipelineSpec,
        table: TableSpec,
        source_root: str | Path,
        output_dir: str | Path | None = None,
    ) -> list[ConversionResult]:
        """Convert every source workbook that belongs to ``table``."""
        sources = self.discover_sources(table, source_root)
        if not sources:
            logger.warning(
                "No %s source files found for %s under %s (glob=%s)",
                table.source_format, table.source_name, source_root, table.source_glob,
            )
            return []

        out_dir = Path(output_dir) if output_dir else PROJECT_ROOT / self.config.paths.staging_csv_dir
        out_dir = out_dir if out_dir.is_absolute() else PROJECT_ROOT / out_dir
        out_dir = out_dir / spec.run_id / table.gcs_folder

        results: list[ConversionResult] = []
        for source_file in sources:
            results.append(self.convert_file(spec, table, source_file, out_dir))
        return results

    def discover_sources(self, table: TableSpec, source_root: str | Path) -> list[Path]:
        """Find workbooks for this table, preferring a source-specific subfolder."""
        root = Path(source_root)
        if not root.exists():
            raise FileNotFoundError(f"Source root not found: {root}")

        pattern = table.source_glob or f"*.{table.source_format}"
        candidates = sorted(p for p in root.rglob(pattern) if p.is_file() and not p.name.startswith("~$"))
        if not candidates:
            return []

        # One root can hold several sources (brushfire vs firebreak share a glob),
        # so keep only files whose path mentions this table's distinctive token.
        keywords = _distinctive_tokens(table)
        if not keywords:
            return candidates
        # No fallback on purpose: loading brushfire files into the firebreak table
        # would be far worse than reporting "no files found".
        return [
            p for p in candidates
            if any(k in re.sub(r"[^a-z0-9]+", "", str(p).lower()) for k in keywords)
        ]

    def convert_file(
        self,
        spec: PipelineSpec,
        table: TableSpec,
        source_file: Path,
        output_dir: Path,
    ) -> ConversionResult:
        suffix = self._file_suffix(table, source_file)
        csv_name = table.csv_file_name(spec.run_id, suffix)
        csv_path = output_dir / csv_name

        frame = self._read_workbook(source_file, table)
        frame = self._map_columns(frame, table, source_file)
        frame = self._add_metadata(frame, table, spec.run_date, source_file)
        frame = frame[table.csv_columns]

        if self.dry_run:
            logger.info(
                "[DRY RUN] would write %s rows -> %s", len(frame), csv_path,
            )
            return ConversionResult(
                source_name=table.source_name, source_file=source_file, csv_path=csv_path,
                row_count=len(frame), columns=list(frame.columns), file_suffix=suffix,
                skipped=True, message="dry run - no file written",
            )

        output_dir.mkdir(parents=True, exist_ok=True)
        self._write_csv(frame, csv_path, source_file)
        logger.info("Converted %s -> %s (%s rows)", source_file.name, csv_path.name, len(frame))
        return ConversionResult(
            source_name=table.source_name, source_file=source_file, csv_path=csv_path,
            row_count=len(frame), columns=list(frame.columns), file_suffix=suffix,
        )

    @staticmethod
    def _file_suffix(table: TableSpec, source_file: Path) -> str | None:
        """Per-state sources become one CSV each: ``AK_Change_Log.xlsx`` -> ``AK``."""
        if not table.filename_state_pattern:
            return None
        match = re.match(r"^([A-Za-z]{2})[_\-]", source_file.stem)
        return match.group(1).upper() if match else re.sub(r"[^A-Za-z0-9]+", "_", source_file.stem)

    # -- reading ------------------------------------------------------------
    def _read_workbook(self, source_file: Path, table: TableSpec) -> pd.DataFrame:
        readable = local_readable_copy(source_file)
        try:
            header_row = self._detect_header_row(readable, table, len(table.file_columns))
            # dtype=str across the board is what preserves leading zeros.
            frame = pd.read_excel(readable, header=header_row, dtype=str, engine="openpyxl")
        finally:
            cleanup_copy(source_file, readable)
        if len(frame):
            # On a 0-row sheet every column looks "all NaN", so only prune when rows exist.
            frame = frame.dropna(axis=1, how="all").dropna(axis=0, how="all")
        frame.columns = [str(c).strip() for c in frame.columns]
        frame = frame.loc[:, [c for c in frame.columns if not c.lower().startswith("unnamed:")]]
        return frame.fillna("")

    def _detect_header_row(self, path: Path, table: TableSpec, expected_columns: int) -> int:
        """Change logs often start with banner rows; find the real header."""
        probe = pd.read_excel(
            path, header=None, nrows=self.config.file_prep.max_header_scan_rows,
            dtype=str, engine="openpyxl",
        )
        expected_tokens = [_token_set(c.name) for c in table.file_columns]
        best_row, best_score = 0, -1
        for row_index in range(len(probe)):
            values = [str(v).strip() for v in probe.iloc[row_index].tolist() if str(v).strip() not in ("", "nan", "None")]
            if not values:
                continue
            score = sum(1 for value in values if _token_set(value) in expected_tokens)
            if len(values) == expected_columns:
                score += 1
            if score > best_score:
                best_row, best_score = row_index, score
        logger.debug("Header row for %s detected at index %s (score %s)", path.name, best_row, best_score)
        return best_row

    # -- column mapping -----------------------------------------------------
    def _map_columns(self, frame: pd.DataFrame, table: TableSpec, source_file: Path) -> pd.DataFrame:
        targets = table.file_columns
        source_headers = list(frame.columns)
        mapping: dict[str, str] = {}
        remaining = list(source_headers)

        # 1. exact / normalized match, 2. token-set match
        for matcher in (
            lambda header, column: normalize_header(header) == normalize_header(column.source_header or column.name),
            lambda header, column: _token_set(header) == _token_set(column.source_header or column.name),
        ):
            for column in targets:
                if column.name in mapping.values():
                    continue
                for header in list(remaining):
                    if matcher(header, column):
                        mapping[header] = column.name
                        remaining.remove(header)
                        break

        unmapped = [c for c in targets if c.name not in mapping.values()]
        if unmapped:
            # 3. positional fallback - only safe when the shapes line up.
            if len(remaining) == len(unmapped):
                for header, column in zip(remaining, unmapped):
                    mapping[header] = column.name
                logger.warning(
                    "%s: mapped %s column(s) positionally: %s",
                    source_file.name, len(unmapped), dict(zip(remaining, [c.name for c in unmapped])),
                )
            else:
                raise ValueError(
                    f"{source_file.name}: cannot map source headers {remaining} to expected columns "
                    f"{[c.name for c in unmapped]}. Set 'source_header' on those columns in the spec."
                )

        renamed = frame.rename(columns=mapping)
        return renamed[[c.name for c in targets]]

    # -- metadata & writing -------------------------------------------------
    def _add_metadata(
        self, frame: pd.DataFrame, table: TableSpec, run_date: date, source_file: Path
    ) -> pd.DataFrame:
        frame = frame.copy()
        for column in table.columns_00:
            if not column.is_metadata:
                continue
            if column.name == table.partition_by:
                frame[column.name] = run_date.isoformat()
            elif column.name == self.config.pipeline.source_file_field:
                frame[column.name] = source_file.name
        return frame

    def _write_csv(self, frame: pd.DataFrame, csv_path: Path, source_file: Path) -> None:
        quote_char = self.config.file_prep.csv_quote_char
        delimiter = self.config.bigquery.external_table_options.field_delimiter
        cleaned = frame.astype(str).replace({"nan": "", "None": "", "NaT": ""})

        if not quote_char:
            # The 00 template sets quote="" so no value may contain the delimiter.
            cleaned = cleaned.map(lambda v: _sanitize(v, delimiter, source_file.name))
            cleaned.to_csv(
                csv_path, index=False, sep=delimiter,
                quoting=csv.QUOTE_NONE, encoding=self.config.file_prep.csv_encoding,
                lineterminator="\n",
            )
        else:
            cleaned.to_csv(
                csv_path, index=False, sep=delimiter, quotechar=quote_char,
                quoting=csv.QUOTE_MINIMAL, encoding=self.config.file_prep.csv_encoding,
                lineterminator="\n",
            )


_WARNED: set[str] = set()


def _sanitize(value: str, delimiter: str, file_name: str) -> str:
    cleaned = value.replace("\r", " ").replace("\n", " ")
    if delimiter in cleaned:
        if file_name not in _WARNED:
            logger.warning("%s: replaced embedded %r in values (csv quote char is empty).", file_name, delimiter)
            _WARNED.add(file_name)
        cleaned = cleaned.replace(delimiter, " ")
    return cleaned.strip()


# -- file validations (Phase 10, file section) ------------------------------
def validate_csv_outputs(
    table: TableSpec, results: list[ConversionResult], report: ValidationReport, dry_run: bool
) -> ValidationReport:
    prefix = f"file.{table.table_00}"
    # A missing source folder is a WARN, not a FAIL: nothing was written, and the
    # BigQuery row-count check is the real gate before anything lands in 01.
    report.add_result(
        f"{prefix}.files_found", bool(results),
        f"{len(results)} source file(s) converted", "no source files were found for this table",
        severity="WARN",
    )

    for result in results:
        tag = f"{prefix}.{result.csv_path.name if result.csv_path else result.source_file.name}"
        report.add_result(
            f"{tag}.row_count", result.row_count > 0,
            f"{result.row_count} rows", "converted file has zero rows",
            row_count=result.row_count,
        )
        report.add_result(
            f"{tag}.columns", result.columns == table.csv_columns,
            "CSV header matches the 00 external table schema",
            f"CSV header {result.columns} != expected {table.csv_columns}",
        )
        if dry_run or result.csv_path is None:
            continue
        report.add_result(
            f"{tag}.exists", result.csv_path.exists(),
            f"{result.csv_path.name} written", f"expected CSV missing: {result.csv_path}",
        )
        if result.csv_path.exists():
            report.extend(_check_leading_zeros(table, result.csv_path))
    return report


def _check_leading_zeros(table: TableSpec, csv_path: Path):
    from ..models.validation import ValidationResult

    results: list[ValidationResult] = []
    zero_columns = table.leading_zero_columns
    if not zero_columns:
        return results

    frame = pd.read_csv(csv_path, dtype=str, nrows=1000, keep_default_na=False)
    for column in zero_columns:
        if column not in frame.columns:
            results.append(ValidationResult(f"file.{csv_path.name}.{column}", "FAIL", "column missing from CSV"))
            continue
        values = [v for v in frame[column].tolist() if v]
        widths = {len(v) for v in values}
        stripped = [v for v in values if v != v.lstrip("0") and v.lstrip("0")]
        results.append(
            ValidationResult(
                f"file.{csv_path.name}.{column}.leading_zeros",
                "PASS" if len(widths) <= 1 or stripped else "WARN",
                f"{len(values)} values, widths={sorted(widths)}, {len(stripped)} with leading zeros",
                {"widths": sorted(widths), "leading_zero_values": len(stripped)},
            )
        )
    return results


__all__ = [
    "XlsxToCsvConverter",
    "ConversionResult",
    "normalize_header",
    "validate_csv_outputs",
]
