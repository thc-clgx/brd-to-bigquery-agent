"""Runs the 00 -> 01 delete-and-reload for a single run."""

from __future__ import annotations

from ..models.pipeline_spec import PipelineSpec, TableSpec
from ..utils.logging_setup import get_logger
from .table_creator import BigQueryExecutor, JobResult

logger = get_logger(__name__)


class LoadRunner:
    """Executes the generated load script and reports before/after row counts."""

    def __init__(self, executor: BigQueryExecutor) -> None:
        self.executor = executor

    def count_00(self, spec: PipelineSpec, table: TableSpec) -> int | None:
        physical = table.physical_name(table.partition_by)
        # No run column in the staged file means every row belongs to this run.
        where = (
            f"\nWHERE SAFE_CAST(NULLIF(TRIM({physical}), '') AS DATE) = DATE '{spec.run_date}'"
            if physical else ""
        )
        sql = (
            f"SELECT COUNT(*) AS row_count\n"
            f"FROM `{spec.project_id}.{spec.dataset_00}.{table.table_00}`{where};"
        )
        rows = self.executor.query_rows(sql, f"count_00_{table.table_00}")
        return int(rows[0]["row_count"]) if rows else None

    def count_01(self, spec: PipelineSpec, table: TableSpec) -> int | None:
        sql = (
            f"SELECT COUNT(*) AS row_count\n"
            f"FROM `{spec.project_id}.{spec.dataset_01}.{table.table_01}`\n"
            f"WHERE {table.partition_by} = DATE '{spec.run_date}';"
        )
        rows = self.executor.query_rows(sql, f"count_01_{table.table_01}")
        return int(rows[0]["row_count"]) if rows else None

    def load(self, spec: PipelineSpec, table: TableSpec, load_sql: str) -> dict:
        """Run the load and return a summary for the execution report."""
        before_00 = self.count_00(spec, table)
        before_01 = self.count_01(spec, table)
        logger.info(
            "%s: 00 rows for %s = %s; 01 rows before load = %s",
            table.table_01, spec.run_date, before_00, before_01,
        )

        result: JobResult = self.executor.execute(load_sql, f"load_01_{table.table_01}")
        after_01 = self.count_01(spec, table) if result.ok else None

        return {
            "table_00": table.table_00,
            "table_01": table.table_01,
            "run_date": str(spec.run_date),
            "rows_00": before_00,
            "rows_01_before": before_01,
            "rows_01_after": after_01,
            "job": result.to_dict(),
        }


__all__ = ["LoadRunner"]
