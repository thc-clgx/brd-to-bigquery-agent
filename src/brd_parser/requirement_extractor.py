"""Rule-based BRD -> :class:`PipelineSpec` extraction.

Deterministic on purpose: no LLM call, no network. The extractor understands the
Core surveillance BRD shape (nested bullets: source -> Fields -> ``name TYPE``,
plus a ``Development Location:`` block holding project / bucket / datasets /
table names).

To swap in an LLM parser later, implement the same
``extract(document) -> PipelineSpec`` contract and register it in
``pipeline_runner``; everything downstream is unchanged.
"""

from __future__ import annotations

import re
from datetime import date

from ..models.pipeline_spec import ColumnSpec, PipelineSpec, TableSpec, normalize_bq_type
from ..utils.config import AppConfig
from ..utils.logging_setup import get_logger
from ..utils.run_date import normalize_run_id, parse_run_date
from .document_reader import BrdDocument, BrdLine, read_document

logger = get_logger(__name__)

_FIELD_LINE = re.compile(
    r"^(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*[:\-]?\s+"
    r"(?P<type>STRING|BYTES|INTEGER|INT64|INT|FLOAT64|FLOAT|DOUBLE|NUMERIC|BIGNUMERIC|"
    r"BOOL|BOOLEAN|DATE|DATETIME|TIMESTAMP|TIME)\b",
    re.IGNORECASE,
)
_TABLE_00 = re.compile(r"^(00_[A-Za-z0-9_]+)$")
_TABLE_01 = re.compile(r"^(01_[A-Za-z0-9_]+)$")
_LEADING_ZERO_HINT = re.compile(r"leading\s+zero", re.IGNORECASE)
_PIPELINE_TYPE = re.compile(r"\(\s*(?P<type>[A-Za-z0-9_]+(?:\s*->\s*[A-Za-z0-9_]+)+)\s*\)")
_STATE_TOKEN = re.compile(r"<\s*state\w*\s*>", re.IGNORECASE)

_GRAIN_SUFFIXES = {
    "county": "county",
    "state": "state",
    "zipcode": "zipcode",
    "zip": "zipcode",
    "property": "property",
}

_STOP_TOKENS = {"the", "a", "an", "of", "for", "and", "log", "logs", "changelog", "changelogs", "change"}


class BrdParseError(ValueError):
    """Raised when a BRD lacks information the framework cannot safely default."""


def _tokens(text: str) -> list[str]:
    """Lowercase alphanumeric tokens, with camelCase split out."""
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", text)
    return [t for t in re.split(r"[^A-Za-z0-9]+", spaced.lower()) if t]


def _key_tokens(text: str) -> set[str]:
    return {t for t in _tokens(text) if t not in _STOP_TOKENS and not t.isdigit()}


def match_score(target: set[str], candidate: set[str]) -> int:
    """How strongly two name token-sets refer to the same thing.

    Shared tokens score highest; a substring hit catches spelling differences
    such as BRD prose ``Fire Break`` vs table name ``firebreak``.
    """
    shared = target & candidate
    score = 3 * len(shared)
    target_joined = "".join(sorted(target))
    candidate_joined = "".join(sorted(candidate))
    for token in candidate - shared:
        if len(token) >= 4 and token in target_joined:
            score += 2
    for token in target - shared:
        if len(token) >= 4 and token in candidate_joined:
            score += 2
    return score


class RequirementExtractor:
    """Turns a :class:`BrdDocument` into a validated :class:`PipelineSpec`."""

    def __init__(self, config: AppConfig, pipeline_type: str = "GCS->00->01") -> None:
        self.config = config
        self.pipeline_type = pipeline_type
        self.warnings: list[str] = []

    # -- public API ---------------------------------------------------------
    def extract(self, document: BrdDocument, run_id: str | None = None) -> PipelineSpec:
        deployment = self._find_deployment_block(document)
        if not deployment:
            raise BrdParseError(
                f"No '{self.pipeline_type}' deployment block found in {document.path.name}. "
                "Expected a 'Development Location:' section with Project / Bucket / Data Sets / Table Names."
            )

        project_id = self._reconcile("project_id", self._scalar(deployment, r"project\s*:\s*(\S+)"), self.config.gcp.project_id)
        bucket = self._reconcile("gcs_bucket", self._scalar(deployment, r"bucket\s*:\s*(\S+)"), self.config.gcs.bucket)
        dataset_00 = self._reconcile("dataset_00", self._scalar(deployment, r"^0*00\s*:\s*(\S+)"), self.config.bigquery.dataset_00)
        dataset_01 = self._reconcile("dataset_01", self._scalar(deployment, r"^0*01\s*:\s*(\S+)"), self.config.bigquery.dataset_01)

        tables_00 = self._collect(deployment, _TABLE_00)
        tables_01 = self._collect(deployment, _TABLE_01)
        folders = [m for m in (self._match(line, r"folder\s*:\s*(\S+)") for line in deployment) if m]

        if not tables_00:
            raise BrdParseError("BRD deployment block lists no 00_* table names.")
        if not tables_01:
            raise BrdParseError("BRD deployment block lists no 01_* table names.")

        schedule = self._scalar(deployment, r"schedule\s*:\s*(.+)")
        historical = self._historical_runs(deployment)
        directives = self._pipeline_directives(document, deployment)
        sources = self._extract_source_sections(document)

        resolved_run_id = normalize_run_id(run_id) if run_id else (historical[-1] if historical else None)
        if not resolved_run_id:
            raise BrdParseError("No run identifier supplied and none found under 'Historical Load:' in the BRD.")

        tables = [
            self._build_table(name_00, tables_01, folders, sources, directives)
            for name_00 in tables_00
        ]

        spec = PipelineSpec(
            environment=self.config.environment,
            project_id=project_id,
            location=self.config.gcp.location,
            gcs_bucket=bucket,
            dataset_00=dataset_00,
            dataset_01=dataset_01,
            run_id=resolved_run_id,
            run_date=parse_run_date(resolved_run_id),
            pipeline_type=self.pipeline_type,
            rerun_strategy=self.config.pipeline.rerun_strategy,
            schedule=schedule,
            historical_runs=historical,
            tables=tables,
            brd_source=document.path.as_posix(),
            warnings=self.warnings,
        )
        logger.info(
            "Extracted spec: %s tables, run_id=%s, run_date=%s",
            len(spec.tables), spec.run_id, spec.run_date,
        )
        return spec

    # -- deployment block ---------------------------------------------------
    def _find_deployment_block(self, document: BrdDocument) -> list[BrdLine]:
        """Lines of the ``Development Location:`` block for our pipeline type."""
        anchors = [line for line in document.lines if line.normalized.startswith("development location")]
        if not anchors:
            return []

        wanted = self._normalize_pipeline_type(self.pipeline_type)
        for anchor in anchors:
            block = document.section(anchor.index)
            if not block:
                block = self._flat_block(document, anchor)
            if self._block_pipeline_type(document, anchor) == wanted:
                return block

        self.warnings.append(
            f"No block explicitly declared pipeline type {self.pipeline_type}; using the first Development Location block."
        )
        first = anchors[0]
        return document.section(first.index) or self._flat_block(document, first)

    @staticmethod
    def _flat_block(document: BrdDocument, anchor: BrdLine) -> list[BrdLine]:
        """Fallback when indent levels are flat: read until the next major heading."""
        collected: list[BrdLine] = []
        for line in document.lines[anchor.index + 1:]:
            if line.normalized.startswith("development location"):
                break
            if line.normalized in {"model results", "model accuracy", "key metrics", "pipeline"}:
                break
            collected.append(line)
        return collected

    def _block_pipeline_type(self, document: BrdDocument, anchor: BrdLine) -> str | None:
        """Nearest preceding 'Pipeline' bullet that declares e.g. (GCS->00->01)."""
        for line in reversed(document.lines[: anchor.index]):
            match = _PIPELINE_TYPE.search(line.text)
            if match and "pipeline" in line.normalized:
                return self._normalize_pipeline_type(match.group("type"))
        return None

    @staticmethod
    def _normalize_pipeline_type(value: str) -> str:
        return re.sub(r"\s*->\s*", "->", value.strip()).upper()

    # -- pipeline directives -----------------------------------------------
    def _pipeline_directives(self, document: BrdDocument, deployment: list[BrdLine]) -> dict:
        """Partition / cluster / metadata directives from the Pipeline section."""
        end = deployment[0].index if deployment else len(document.lines)
        window = [line for line in document.lines[:end]]

        partition_by = self.config.pipeline.partition_field
        cluster_candidates: list[str] = []
        load_date_field: str | None = None
        wildcard = False

        for line in window:
            text = line.text
            lowered = line.normalized
            if lowered.startswith("partition by"):
                token = text.split("by", 1)[1].strip()
                token = re.split(r"[,(]", token)[0].strip()
                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token):
                    partition_by = token
            elif lowered.startswith("cluster by"):
                tail = text.split("by", 1)[1]
                cluster_candidates.extend(
                    t for t in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", tail)
                    if t.lower() not in {"or", "as", "appropriate", "based", "on", "table", "contents", "and"}
                )
            elif "wildcard" in lowered or "*.csv" in lowered:
                wildcard = True
            match = re.search(r"add\s+(LOAD_DATE_\d+)", text, re.IGNORECASE)
            if match:
                load_date_field = match.group(1).upper()

        return {
            "partition_by": partition_by,
            "cluster_candidates": cluster_candidates,
            "load_date_field": load_date_field or self.config.pipeline.load_date_field,
            "wildcard": wildcard,
        }

    # -- source sections ----------------------------------------------------
    def _extract_source_sections(self, document: BrdDocument) -> list[dict]:
        """Every ``<Source> / Fields / name TYPE`` group found in the BRD."""
        sections: list[dict] = []
        for line in document.lines:
            if line.normalized != "fields":
                continue
            heading = self._parent_heading(document, line)
            if heading is None:
                continue
            columns = self._columns_under(document, line)
            if not columns:
                continue
            sections.append(
                {
                    "name": heading.text,
                    "tokens": _key_tokens(heading.text),
                    "columns": columns,
                    "location": self._source_location(document, heading, line),
                    "heading_index": heading.index,
                }
            )
        logger.info("Found %s source field sections: %s", len(sections), [s["name"] for s in sections])
        return sections

    @staticmethod
    def _parent_heading(document: BrdDocument, fields_line: BrdLine) -> BrdLine | None:
        for line in reversed(document.lines[: fields_line.index]):
            if line.level < fields_line.level:
                return line
        return None

    def _columns_under(self, document: BrdDocument, fields_line: BrdLine) -> list[ColumnSpec]:
        columns: list[ColumnSpec] = []
        for line in document.lines[fields_line.index + 1:]:
            if line.level <= fields_line.level:
                break
            match = _FIELD_LINE.match(line.text.strip())
            if match:
                columns.append(
                    ColumnSpec(
                        name=match.group("name"),
                        type=normalize_bq_type(match.group("type")),
                        source_header=match.group("name"),
                    )
                )
            elif columns and _LEADING_ZERO_HINT.search(line.text):
                columns[-1].preserve_leading_zeros = True
        # Belt and braces: config-declared identifier columns always stay STRING.
        for column in columns:
            if column.name in self.config.file_prep.leading_zero_columns:
                column.preserve_leading_zeros = True
                if column.type != "STRING":
                    self.warnings.append(
                        f"Forcing {column.name} to STRING to preserve leading zeros (BRD said {column.type})."
                    )
                    column.type = "STRING"
        return columns

    @staticmethod
    def _source_location(document: BrdDocument, heading: BrdLine, fields_line: BrdLine) -> str | None:
        for line in document.lines[heading.index:fields_line.index]:
            if line.text.strip().lower().startswith("gs://"):
                return line.text.strip()
        return None

    # -- table assembly -----------------------------------------------------
    def _build_table(
        self,
        table_00: str,
        tables_01: list[str],
        folders: list[str],
        sources: list[dict],
        directives: dict,
    ) -> TableSpec:
        stem = table_00[3:] if table_00.startswith("00_") else table_00
        stem_tokens = _key_tokens(stem)

        table_01 = self._best_match(stem_tokens, tables_01, lambda n: _key_tokens(n[3:]))
        if table_01 is None:
            raise BrdParseError(f"No 01 table in the BRD matches 00 table {table_00!r}.")

        gcs_folder = self._best_match(stem_tokens, folders, _key_tokens) or table_00
        source = self._best_match(stem_tokens, sources, lambda s: s["tokens"])
        if source is None:
            raise BrdParseError(f"No BRD 'Fields' section matches table {table_00!r}.")

        columns = [c.model_copy(deep=True) for c in source["columns"]]
        grain = self._infer_grain(table_01, columns)
        columns.extend(self._metadata_columns(directives["load_date_field"]))

        cluster_by = [c for c in directives["cluster_candidates"] if c in {col.name for col in columns}]
        if not cluster_by:
            fallback = {"county": "countyFIPSCode", "state": "stateCode"}.get(grain)
            if fallback and fallback in {col.name for col in columns}:
                cluster_by = [fallback]

        location = source.get("location")
        per_state = bool(location and _STATE_TOKEN.search(location))

        return TableSpec(
            source_name=source["name"],
            grain=grain,
            table_00=table_00,
            table_01=table_01,
            gcs_folder=gcs_folder,
            file_pattern=f"{stem}_",
            schema=columns,
            partition_by=directives["partition_by"],
            cluster_by=cluster_by,
            source_format=self.config.pipeline.source_format,
            target_format=self.config.pipeline.target_format,
            wildcard_required=True,
            rerun_strategy=self.config.pipeline.rerun_strategy,
            source_glob=self._source_glob(location),
            filename_state_pattern="stateAbbrev" if per_state else None,
            source_location_hint=location,
            brd_section=source["name"],
        )

    def _metadata_columns(self, load_date_field: str) -> list[ColumnSpec]:
        pipeline = self.config.pipeline
        return [
            ColumnSpec(
                name=pipeline.run_identifier_field, type="DATE", is_metadata=True, nullable=False,
                description="Run identifier (YYYYMM) converted to DATE, e.g. 2025-04-01",
            ),
            ColumnSpec(
                name=pipeline.source_file_field, type="STRING", is_metadata=True,
                description="Name of the source workbook this row came from",
            ),
            ColumnSpec(
                name=load_date_field, type="DATE", is_metadata=True, added_at_load=True, nullable=False,
                description="Date the row was loaded into the 01 table",
            ),
        ]

    @staticmethod
    def _source_glob(location: str | None) -> str | None:
        if not location:
            return None
        file_name = location.rstrip("/").split("/")[-1]
        return _STATE_TOKEN.sub("*", file_name) or None

    @staticmethod
    def _infer_grain(table_01: str, columns: list[ColumnSpec]) -> str:
        suffix = table_01.rsplit("_", 1)[-1].lower()
        if suffix in _GRAIN_SUFFIXES:
            return _GRAIN_SUFFIXES[suffix]
        names = {c.name.lower() for c in columns}
        if "countyfipscode" in names:
            return "county"
        if "statecode" in names:
            return "state"
        return "other"

    @staticmethod
    def _best_match(target: set[str], candidates: list, tokenizer) -> object | None:
        """Pick the candidate whose name refers most strongly to ``target``."""
        best, best_score = None, 0
        for candidate in candidates:
            score = match_score(target, set(tokenizer(candidate)))
            if score > best_score:
                best, best_score = candidate, score
        return best

    # -- small helpers ------------------------------------------------------
    @staticmethod
    def _match(line: BrdLine, pattern: str) -> str | None:
        match = re.search(pattern, line.text.strip(), re.IGNORECASE)
        return match.group(1).strip().rstrip(".,;") if match else None

    def _scalar(self, lines: list[BrdLine], pattern: str) -> str | None:
        for line in lines:
            value = self._match(line, pattern)
            if value:
                return value
        return None

    @staticmethod
    def _collect(lines: list[BrdLine], pattern: re.Pattern) -> list[str]:
        found: list[str] = []
        for line in lines:
            match = pattern.match(line.text.strip())
            if match and match.group(1) not in found:
                found.append(match.group(1))
        return found

    @staticmethod
    def _historical_runs(lines: list[BrdLine]) -> list[str]:
        runs: list[str] = []
        capturing = False
        for line in lines:
            if line.normalized.startswith("historical load"):
                capturing = True
                continue
            if not capturing:
                continue
            token = line.text.strip()
            try:
                normalized = normalize_run_id(token)
            except ValueError:
                if runs:
                    break
                continue
            if normalized not in runs:
                runs.append(normalized)
        return sorted(runs)

    def _reconcile(self, field: str, from_brd: str | None, from_config: str | None) -> str:
        """The BRD drives the value; a non-null config entry pins/overrides it."""
        if from_config is None:
            if not from_brd:
                raise BrdParseError(
                    f"{field}: not found in the BRD and not pinned in config. "
                    f"Add it to the BRD's Development Location section, or set it in the config."
                )
            return from_brd
        if from_brd and from_brd != from_config:
            self.warnings.append(
                f"{field}: BRD says {from_brd!r} but config pins {from_config!r}. Using the config value."
            )
        elif not from_brd:
            self.warnings.append(f"{field}: not found in BRD, using pinned config value {from_config!r}.")
        return from_config


def extract_pipeline_spec(
    brd_path: str,
    config: AppConfig,
    run_id: str | None = None,
    pipeline_type: str = "GCS->00->01",
) -> PipelineSpec:
    """Convenience wrapper: read the BRD and extract the spec in one call."""
    document = read_document(brd_path)
    return RequirementExtractor(config, pipeline_type=pipeline_type).extract(document, run_id=run_id)


__all__ = ["BrdParseError", "RequirementExtractor", "extract_pipeline_spec"]
