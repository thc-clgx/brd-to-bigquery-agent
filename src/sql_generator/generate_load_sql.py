"""00 -> 01 delete-and-reload SQL generation."""

from __future__ import annotations

from ..file_prep.source_profiler import zero_pad_width
from ..models.pipeline_spec import ColumnSpec, PipelineSpec, TableSpec
from ..utils.config import AppConfig
from .render_template import SqlRenderer

TEMPLATE = "01_load_replace_run_template.sql.j2"

_NUMERIC = {"INT64", "INTEGER", "FLOAT64", "NUMERIC", "BIGNUMERIC"}


def cast_expression(
    column: ColumnSpec, alias: str = "src", all_strings: bool = True, physical: str | None = None
) -> str:
    """SELECT expression that turns a 00 STRING column into its 01 type."""
    raw = f"{alias}.{physical or column.name}"
    if not all_strings:
        return raw

    trimmed = f"NULLIF(TRIM({raw}), '')"
    if column.type == "STRING":
        width = zero_pad_width(column.name) if column.preserve_leading_zeros else None
        # Some staged files lost their leading zeros in Excel; restore them here.
        return f"LPAD({trimmed}, {width}, '0')" if width else trimmed
    if column.type in _NUMERIC:
        # Strip thousands separators and currency noise Excel likes to add.
        cleaned = f"REGEXP_REPLACE({trimmed}, r'[,$%\\s]', '')"
        target = "INT64" if column.type in {"INT64", "INTEGER"} else column.type
        return f"SAFE_CAST({cleaned} AS {target})"
    return f"SAFE_CAST({trimmed} AS {column.type})"


def generate_load_sql(
    spec: PipelineSpec,
    table: TableSpec,
    config: AppConfig,
    renderer: SqlRenderer | None = None,
    run_date=None,
) -> str:
    """Render the transactional DELETE-then-INSERT script for one run."""
    renderer = renderer or SqlRenderer(config.paths.templates_dir)
    all_strings = config.bigquery.external_all_strings
    effective_run_date = run_date or spec.run_date

    columns = []
    for column in table.columns_01:
        physical = table.physical_name(column.name)
        if column.added_at_load:
            expr = "CURRENT_DATE()"
        elif column.name == table.partition_by:
            # Pin every row of the run to the canonical run_date, whatever the file says.
            expr = f"DATE '{effective_run_date}'"
        elif physical is None:
            # Column the BRD requires but the staged file lacks.
            expr = (
                "REGEXP_EXTRACT(src._FILE_NAME, r'[^/]+$')"
                if column.name == config.pipeline.source_file_field
                else "CAST(NULL AS STRING)"
            )
        else:
            expr = cast_expression(column, all_strings=all_strings, physical=physical)
        columns.append({"name": column.name, "expr": expr})

    partition_physical = table.physical_name(table.partition_by)
    partition_column = next(
        (c for c in table.columns_00 if c.name == table.partition_by), None
    )
    if partition_column is not None and partition_physical is not None:
        run_filter_expr = cast_expression(
            partition_column, all_strings=all_strings, physical=partition_physical
        )
    else:
        # No run column in the file: the whole object is the run, so do not filter.
        run_filter_expr = None

    context = {
        "project_id": spec.project_id,
        "dataset_00": spec.dataset_00,
        "dataset_01": spec.dataset_01,
        "table_00": table.table_00,
        "table_01": table.table_01,
        "columns": columns,
        "partition_field": table.partition_by,
        "run_filter_expr": run_filter_expr,
        "run_date": effective_run_date.isoformat(),
        "run_id": spec.run_id,
        "rerun_strategy": table.rerun_strategy,
        "generated_at": spec.generated_at.isoformat(timespec="seconds"),
    }
    return renderer.render(TEMPLATE, context)


def sql_file_name(table: TableSpec, run_id: str) -> str:
    return f"load_{table.table_01}_{run_id}.sql"


def is_destructive(sql: str) -> bool:
    upper = sql.upper()
    return "DELETE FROM" in upper or "TRUNCATE TABLE" in upper or "DROP TABLE" in upper
