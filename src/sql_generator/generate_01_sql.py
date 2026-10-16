"""01 managed table DDL generation."""

from __future__ import annotations

from ..models.pipeline_spec import PipelineSpec, TableSpec
from ..utils.config import AppConfig
from .render_template import SqlRenderer

TEMPLATE = "01_create_table_template.sql.j2"


def generate_01_sql(
    spec: PipelineSpec,
    table: TableSpec,
    config: AppConfig,
    renderer: SqlRenderer | None = None,
) -> str:
    """Render ``CREATE TABLE IF NOT EXISTS`` for the partitioned/clustered 01 table."""
    renderer = renderer or SqlRenderer(config.paths.templates_dir)

    columns = [
        {
            "name": column.name,
            "type": column.type,
            "nullable": column.nullable,
            "description": column.description,
        }
        for column in table.columns_01
    ]

    context = {
        "project_id": spec.project_id,
        "dataset_01": spec.dataset_01,
        "table_01": table.table_01,
        "source_name": table.source_name,
        "grain": table.grain,
        "brd_source": spec.brd_source or "n/a",
        "generated_at": spec.generated_at.isoformat(timespec="seconds"),
        "columns": columns,
        "partition_by": table.partition_by,
        "cluster_by": table.cluster_by,
    }
    return renderer.render(TEMPLATE, context)


def sql_file_name(table: TableSpec) -> str:
    return f"{table.table_01}.sql"
