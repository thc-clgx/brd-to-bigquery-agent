"""Managed 00-table DDL and safe external-stage load SQL generation."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from ..sql_generator.render_template import SqlRenderer, write_sql
from ..utils.config import PROJECT_ROOT
from .models import FeedContract, RainfallPipelineSpec
from .processing import PreparedSource

_UNRESOLVED = re.compile(r"\{\{|\}\}|<\s*(?:approved|project|bucket|dataset|prefix)[^>]*>", re.I)


def generate_00_sql(
    spec: RainfallPipelineSpec,
    feed: FeedContract,
    prepared: list[PreparedSource],
    renderer: SqlRenderer | None = None,
    schema_plan: dict[str, Any] | None = None,
) -> str:
    """Render one stage-external plus managed-00 script for an approved target."""
    if not all((spec.project_id, spec.location, spec.gcs_bucket, spec.dataset_00)):
        raise ValueError("Project, location, landing bucket, and 00 dataset must be configured before SQL generation.")
    if not spec.dataset_00_is_approved:
        raise ValueError("The BRD-proposed 00 dataset must be explicitly approved in BQ_DATASET_00.")
    feed_sources = [
        source for source in prepared
        if source.feed_id == feed.feed_id and source.item.load_eligible
    ]
    if not feed_sources:
        raise ValueError(f"No eligible source objects are available for {feed.feed_id}.")
    if any(source.proposed_object_uri is None for source in feed_sources):
        raise ValueError(f"A source object URI is unresolved for {feed.feed_id}.")

    renderer = renderer or SqlRenderer()
    metadata = {column.name: column for column in spec.metadata_columns}
    columns = [
        {
            "name": column.name,
            "type": column.type,
            "nullable": column.nullable,
            "description": column.description,
        }
        for column in feed.source_columns
    ]
    target_columns = columns + [
        {
            "name": column.name,
            "type": column.type,
            "nullable": False,
            "description": column.description,
        }
        for column in metadata.values()
    ]
    source_names = [column.name for column in feed.source_columns]
    table_name = feed.table_00
    stage_table = f"stg_{table_name}_{spec.run_id}"
    uris = sorted({source.proposed_object_uri for source in feed_sources if source.proposed_object_uri})
    file_map = {
        source.staged_file_name: source.item.source_file
        for source in feed_sources
        if source.staged_file_name
    }
    context: dict[str, Any] = {
        "project_id": spec.project_id,
        "location": spec.location,
        "dataset_00": spec.dataset_00,
        "table_00": table_name,
        "stage_table": stage_table,
        "feed_name": feed.source_name,
        "source_columns": columns,
        "target_columns": target_columns,
        "source_names": source_names,
        "partition_field": feed.partition_field,
        "cluster_fields": feed.cluster_fields,
        "source_uris": uris,
        "file_name_cases": [
            {"staged_name": staged_name, "source_name": source_name}
            for staged_name, source_name in sorted(file_map.items())
        ],
        "ingested_at_utc": spec.ingested_at_utc,
        "run_date": spec.run_date.isoformat(),
        "source_file_names": sorted({source.item.source_file for source in feed_sources}),
        "alter_statements": (schema_plan or {}).get("alter_statements", []),
        "brd_file": (
            spec.brd_file.replace("*/", "* /").replace("\r", " ").replace("\n", " ")
        ),
        "brd_sha256": spec.brd_sha256,
        "template_version": "rainfall_00_v1",
    }
    sql = renderer.render("00_rainfall_managed_ingestion.sql.j2", context)
    if _UNRESOLVED.search(sql):
        raise ValueError(f"Generated SQL for {table_name} contains an unresolved template placeholder.")
    return sql


def write_00_sql(
    sql: str,
    feed: FeedContract,
    output_dir: str | Path | None = None,
) -> Path:
    directory = Path(output_dir) if output_dir else PROJECT_ROOT / "generated" / "sql" / "00"
    return write_sql(sql, directory, f"{feed.table_00}.sql")


def plan_schema_evolution(
    existing_columns: list[dict[str, Any]] | None,
    feed: FeedContract,
    project_id: str | None = None,
    dataset_id: str | None = None,
) -> dict[str, Any]:
    """Plan only explicit, nullable, additive schema changes."""
    expected = {column.name: column for column in feed.source_columns}
    expected.update({
        "source_file_name": None,
        "ingested_at": None,
        "run_date": None,
    })
    if existing_columns is None:
        return {
            "table": feed.table_00,
            "action": "CREATE_TABLE",
            "additions": [],
            "preserved_historical_columns": [],
            "existing_schema_checked": False,
            "reason": "No existing BigQuery schema was provided; cloud inspection was not attempted.",
        }

    existing = {item["name"]: item for item in existing_columns}
    historical = sorted(set(existing) - set(expected))
    additions = []
    for name, column in expected.items():
        if name not in existing:
            if column is None:
                return {
                    "table": feed.table_00,
                    "action": "QUARANTINE",
                    "additions": [],
                    "preserved_historical_columns": sorted(existing),
                    "alter_statements": [],
                    "existing_schema_checked": True,
                    "reason": f"Required metadata column {name} is missing from the existing table.",
                }
            if not column.nullable:
                return {
                    "table": feed.table_00,
                    "action": "QUARANTINE",
                    "additions": [],
                    "preserved_historical_columns": sorted(existing),
                    "alter_statements": [],
                    "existing_schema_checked": True,
                    "reason": f"New field {name} is not nullable; only nullable additions are automatic.",
                }
            additions.append({"name": name, "type": column.type, "nullable": True})
            continue
        actual = existing[name]
        expected_type = column.type if column is not None else {
            "source_file_name": "STRING", "ingested_at": "TIMESTAMP", "run_date": "DATE"
        }[name]
        expected_nullable = column.nullable if column is not None else False
        if str(actual.get("type", "")).upper() != expected_type:
            return {
                "table": feed.table_00,
                "action": "QUARANTINE",
                "additions": [],
                "preserved_historical_columns": sorted(existing),
                "alter_statements": [],
                "existing_schema_checked": True,
                "reason": (
                    f"Incompatible type for {name}: existing {actual.get('type')}, "
                    f"BRD {expected_type}; no type alteration is permitted."
                ),
            }
        if bool(actual.get("nullable", True)) != expected_nullable:
            return {
                "table": feed.table_00,
                "action": "QUARANTINE",
                "additions": [],
                "preserved_historical_columns": sorted(existing),
                "alter_statements": [],
                "existing_schema_checked": True,
                "reason": f"Nullability conflict for {name}; no nullability change is permitted.",
            }
    alter_statements = [
        (
            f"ALTER TABLE `{project_id}.{dataset_id}.{feed.table_00}` "
            f"ADD COLUMN IF NOT EXISTS {addition['name']} {addition['type']};"
        )
        for addition in additions
    ] if project_id and dataset_id else []
    return {
        "table": feed.table_00,
        "action": "ADD_NULLABLE_COLUMNS" if additions else "UNCHANGED",
        "additions": additions,
        "preserved_historical_columns": sorted(existing),
        "alter_statements": alter_statements,
        "existing_schema_checked": True,
        "reason": "Existing fields and rows are preserved; only explicit nullable BRD fields may be added.",
    }


__all__ = ["generate_00_sql", "plan_schema_evolution", "write_00_sql"]
