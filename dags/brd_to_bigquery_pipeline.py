"""Airflow DAG: BRD -> GCS -> 00 -> 01 for surveillance change logs.

Dev-friendly and fully parameterized. Nothing mutates by default: ``dry_run``
starts as ``True`` and the approval task rejects unless the run is triggered
with the correct ``approval_token``.

Trigger with a config such as::

    {
      "brd_path": "<mounted-path-to-the-BRD>",
      "run_id": "<run-identifier>",
      "source_root": "<mounted-path-to-source-files>",
      "dry_run": true,
      "approval_token": ""
    }

Deployment: sync this file to the DAGs folder and the ``brd_to_bigquery_agent``
package to a location on ``PYTHONPATH`` (e.g. ``/home/airflow/gcs/dags/``).
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.decorators import dag, task

PROJECT_ROOT = Path(os.environ.get("BRD_AGENT_HOME", Path(__file__).resolve().parents[1]))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_ARGS = {
    "owner": "surveillance-data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

PARAMS = {
    "brd_path": "",
    "config_path": "configs/dev.yaml",
    "run_id": "2026_04",
    "source_root": "",
    "dry_run": True,
    "approval_token": "",
    "pipeline_type": "GCS->00->01",
    # "" defers to llm.extraction_mode in the config.
    "extraction_mode": "",
}


def _runner(params: dict[str, Any]):
    """Build a PipelineRunner from the DAG run params."""
    from src.orchestrator.pipeline_runner import PipelineRunner, RunOptions

    options = RunOptions(
        brd_path=params["brd_path"],
        config_path=params["config_path"],
        run_id=params["run_id"] or None,
        dry_run=bool(params["dry_run"]),
        source_root=params["source_root"] or None,
        approval_token=params["approval_token"] or None,
        non_interactive=True,
        pipeline_type=params["pipeline_type"],
        extraction_mode=params.get("extraction_mode") or None,
    )
    return PipelineRunner(options)


@dag(
    dag_id="brd_to_bigquery_pipeline",
    description="Parse an approved BRD and build/load its GCS -> 00 -> 01 tables.",
    schedule=None,  # BRD: annual, triggered when files land in the AMP'd bucket.
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params=PARAMS,
    max_active_runs=1,
    tags=["surveillance", "wildfire", "brd", "bigquery"],
)
def brd_to_bigquery_pipeline():
    @task
    def parse_brd(**context) -> str:
        runner = _runner(context["params"])
        spec = runner.parse_brd()
        return spec.save(runner.config.paths.generated_specs_dir).as_posix()

    @task
    def extract_pipeline_spec(spec_path: str) -> str:
        """The spec JSON is the contract every downstream task reads."""
        from src.models.pipeline_spec import PipelineSpec

        spec = PipelineSpec.load(spec_path)
        return f"{len(spec.tables)} tables for run {spec.run_id} ({spec.run_date})"

    @task
    def validate_spec(spec_path: str, **context) -> dict:
        from src.brd_parser.spec_validator import validate_spec as _validate
        from src.models.pipeline_spec import PipelineSpec
        from src.utils.config import load_config

        config = load_config(context["params"]["config_path"])
        report = _validate(PipelineSpec.load(spec_path), config)
        report.raise_if_failed("Pipeline spec validation")
        return report.to_dict()

    @task
    def approval_gate(spec_path: str, **context) -> dict:
        """Human gate.

        Today: the run must be triggered with the correct ``approval_token``.
        Swap this body for a Jira transition check, a Teams adaptive-card
        response or a GitHub PR review when that integration exists.
        """
        from src.approval.approval_gate import ApprovalGate, summarize_spec
        from src.models.pipeline_spec import PipelineSpec
        from src.utils.config import load_config

        params = context["params"]
        config = load_config(params["config_path"])
        spec = PipelineSpec.load(spec_path)
        gate = ApprovalGate(
            config=config,
            dry_run=bool(params["dry_run"]),
            interactive=False,
            supplied_token=params["approval_token"] or None,
        )
        return gate.require("airflow_pipeline_run", summarize_spec(spec)).to_dict()

    @task
    def prepare_xlsx_to_csv(spec_path: str, **context) -> dict[str, list[str]]:
        from src.file_prep.xlsx_to_csv import XlsxToCsvConverter
        from src.models.pipeline_spec import PipelineSpec
        from src.utils.config import load_config

        params = context["params"]
        if not params["source_root"]:
            return {}

        config = load_config(params["config_path"])
        spec = PipelineSpec.load(spec_path)
        converter = XlsxToCsvConverter(config, dry_run=bool(params["dry_run"]))
        prepared: dict[str, list[str]] = {}
        for table in spec.tables:
            results = converter.convert_table(spec, table, params["source_root"])
            prepared[table.source_name] = [
                r.csv_path.as_posix() for r in results if r.csv_path and not r.skipped
            ]
        return prepared

    @task
    def upload_csv_to_gcs(spec_path: str, prepared: dict[str, list[str]], **context) -> list[dict]:
        from src.file_prep.gcs_uploader import GcsUploader
        from src.models.pipeline_spec import PipelineSpec
        from src.utils.config import load_config

        params = context["params"]
        config = load_config(params["config_path"])
        spec = PipelineSpec.load(spec_path)
        uploader = GcsUploader(config, dry_run=bool(params["dry_run"]))
        for table in spec.tables:
            files = [Path(p) for p in prepared.get(table.source_name, [])]
            if files:
                uploader.upload_table_files(spec, table, files)
        return uploader.audit

    @task
    def profile_staged_sources(spec_path: str, uploads: list[dict], **context) -> str:
        """Record the real header layout of the staged CSVs.

        The BRD states the contract; the staged files do not always honour it.
        Profiling keeps the 00 table faithful to the file so the 00 -> 01 load
        can normalise, instead of binding declared names on positionally.
        Returns the path of the updated spec, which downstream tasks use.
        """
        from src.file_prep.gcs_uploader import GcsUploader
        from src.file_prep.source_profiler import profile_table
        from src.models.pipeline_spec import PipelineSpec
        from src.utils.config import load_config

        params = context["params"]
        if bool(params["dry_run"]):
            return spec_path

        config = load_config(params["config_path"])
        spec = PipelineSpec.load(spec_path)
        client = GcsUploader(config, dry_run=False).client()
        for table in spec.tables:
            profile = profile_table(client, spec.gcs_bucket, table)
            if profile is None:
                continue
            table.source_headers = profile.headers
            table.source_map = profile.mapping
            table.source_missing = profile.missing
            table.source_uri = f"gs://{spec.gcs_bucket}/{table.gcs_folder}/*.csv"
        return str(spec.save(Path(config.paths.generated_specs_dir)))

    @task
    def generate_sql(spec_path: str, **context) -> dict[str, dict[str, str]]:
        from src.models.pipeline_spec import PipelineSpec
        from src.sql_generator import generate_00_sql, generate_01_sql, generate_load_sql
        from src.sql_generator.render_template import SqlRenderer, write_sql
        from src.utils.config import load_config

        config = load_config(context["params"]["config_path"])
        spec = PipelineSpec.load(spec_path)
        renderer = SqlRenderer(config.paths.templates_dir)
        sql_dir = Path(config.paths.generated_sql_dir)

        rendered: dict[str, dict[str, str]] = {}
        for table in spec.tables:
            statements = {
                "00": generate_00_sql(spec, table, config, renderer),
                "01": generate_01_sql(spec, table, config, renderer),
                "load": generate_load_sql(spec, table, config, renderer),
            }
            write_sql(statements["00"], sql_dir / "00", f"{table.table_00}.sql")
            write_sql(statements["01"], sql_dir / "01", f"{table.table_01}.sql")
            write_sql(statements["load"], sql_dir / "load", f"load_{table.table_01}_{spec.run_id}.sql")
            rendered[table.source_name] = statements
        return rendered

    @task
    def create_00_external_tables(spec_path: str, sql: dict[str, dict[str, str]], **context) -> list[dict]:
        from src.bigquery.table_creator import TableCreator
        from src.models.pipeline_spec import PipelineSpec

        executor, config = _executor(context["params"])
        spec = PipelineSpec.load(spec_path)
        creator = TableCreator(executor)
        creator.ensure_datasets(spec)
        for table in spec.tables:
            creator.create_00(table, sql[table.source_name]["00"])
        return executor.audit

    @task
    def create_01_tables(spec_path: str, sql: dict[str, dict[str, str]], **context) -> list[dict]:
        from src.bigquery.table_creator import TableCreator
        from src.models.pipeline_spec import PipelineSpec

        executor, _ = _executor(context["params"])
        spec = PipelineSpec.load(spec_path)
        creator = TableCreator(executor)
        for table in spec.tables:
            creator.create_01(table, sql[table.source_name]["01"])
        return executor.audit

    @task
    def load_01_replace_run(spec_path: str, sql: dict[str, dict[str, str]], approval: dict, **context) -> list[dict]:
        """DESTRUCTIVE: deletes the run_date partition, then reinserts it."""
        from src.bigquery.load_runner import LoadRunner
        from src.models.pipeline_spec import PipelineSpec

        if not approval.get("approved"):
            raise RuntimeError("Load blocked: the approval gate did not approve this run.")

        executor, _ = _executor(context["params"])
        executor.allow_destructive = True  # unlocked only because approval passed
        spec = PipelineSpec.load(spec_path)
        runner = LoadRunner(executor)
        return [runner.load(spec, t, sql[t.source_name]["load"]) for t in spec.tables]

    @task
    def run_validations(spec_path: str, **context) -> dict:
        from src.bigquery.validations import run_bigquery_validations
        from src.models.pipeline_spec import PipelineSpec

        executor, _ = _executor(context["params"])
        report = run_bigquery_validations(executor, PipelineSpec.load(spec_path))
        return report.to_dict()

    @task(trigger_rule="all_done")
    def publish_report(
        spec_path: str, uploads: list[dict], loads: list[dict], validations: dict, **context
    ) -> str:
        from src.audit.execution_report import ExecutionReport
        from src.models.pipeline_spec import PipelineSpec
        from src.utils.config import load_config

        params = context["params"]
        config = load_config(params["config_path"])
        spec = PipelineSpec.load(spec_path)

        report = ExecutionReport(
            run_id=spec.run_id, environment=config.environment, dry_run=bool(params["dry_run"])
        )
        report.spec_path = spec_path
        report.gcs_uploads = uploads or []
        report.validations = {"bigquery": validations or {}}
        report.step("airflow_dag_run", "OK", dag_run_id=str(context["run_id"]), loads=loads or [])
        report.finish()
        return report.save(config.paths.generated_reports_dir).as_posix()

    def _executor(params: dict[str, Any]):
        from src.bigquery.table_creator import BigQueryExecutor
        from src.utils.config import load_config

        config = load_config(params["config_path"])
        return (
            BigQueryExecutor(
                config=config,
                dry_run=bool(params["dry_run"]),
                allow_destructive=config.safety.allow_destructive,
            ),
            config,
        )

    spec_path = parse_brd()
    summary = extract_pipeline_spec(spec_path)
    spec_ok = validate_spec(spec_path)
    approval = approval_gate(spec_path)

    prepared = prepare_xlsx_to_csv(spec_path)
    uploads = upload_csv_to_gcs(spec_path, prepared)
    # Profiling rewrites the spec, so everything downstream reads its output.
    profiled_path = profile_staged_sources(spec_path, uploads)
    sql = generate_sql(profiled_path)
    created_00 = create_00_external_tables(profiled_path, sql)
    created_01 = create_01_tables(profiled_path, sql)
    loads = load_01_replace_run(profiled_path, sql, approval)
    validations = run_validations(profiled_path)
    final = publish_report(profiled_path, uploads, loads, validations)

    summary >> spec_ok >> approval >> prepared >> uploads
    uploads >> profiled_path >> sql >> created_00 >> created_01 >> loads >> validations >> final


dag_instance = brd_to_bigquery_pipeline()
