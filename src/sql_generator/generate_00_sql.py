"""00 external table DDL generation."""

from __future__ import annotations

from ..models.pipeline_spec import PipelineSpec, TableSpec
from ..utils.config import AppConfig
from .render_template import SqlRenderer

TEMPLATE = "00_external_table_template.sql.j2"


def generate_00_sql(
    spec: PipelineSpec,
    table: TableSpec,
    config: AppConfig,
    renderer: SqlRenderer | None = None,
) -> str:
    """Render ``CREATE OR REPLACE EXTERNAL TABLE`` for one source.

    Following the Core ``00_CT_template.sql`` convention, every 00 column is
    STRING; typing happens in the 00 -> 01 load.
    """
    renderer = renderer or SqlRenderer(config.paths.templates_dir)
    all_strings = config.bigquery.external_all_strings

    if table.is_profiled:
        # Mirror the real file: declared names must line up positionally with it.
        columns = [{"name": header, "type": "STRING"} for header in table.source_headers]
    else:
        columns = [
            {"name": column.name, "type": "STRING" if all_strings else column.type}
            for column in table.columns_00
        ]

    context = {
        "project_id": spec.project_id,
        "dataset_00": spec.dataset_00,
        "table_00": table.table_00,
        "source_name": table.source_name,
        "grain": table.grain,
        "brd_source": spec.brd_source or "n/a",
        "generated_at": spec.generated_at.isoformat(timespec="seconds"),
        "columns": columns,
        "options": config.bigquery.external_table_options.model_dump(),
        "uris": [table.gcs_uri_wildcard(spec.gcs_bucket)],
    }
    return renderer.render(TEMPLATE, context)


def sql_file_name(table: TableSpec) -> str:
    return f"{table.table_00}.sql"
