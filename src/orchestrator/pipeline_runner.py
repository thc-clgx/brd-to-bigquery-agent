"""BRD -> BigQuery pipeline orchestrator and CLI.

Example
-------
    python -m src.orchestrator.pipeline_runner \\
        --brd-path input/WFRS_Surveillance_Detailed_Plan_20260903.docx \\
        --config configs/dev.yaml \\
        --run-id 2026_04 \\
        --dry-run true

Stages run in order and each can be selected individually
(``--generate-sql-only``, ``--prepare-files``, ``--upload-to-gcs``,
``--create-tables``, ``--load-01``, ``--validate``). With no stage flags the
full pipeline runs.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..approval.approval_gate import ApprovalGate, ApprovalRejected, summarize_spec
from ..audit.execution_report import ExecutionReport
from ..bigquery.load_runner import LoadRunner
from ..bigquery.table_creator import BigQueryExecutor, TableCreator
from ..bigquery.validations import run_bigquery_validations
from ..brd_parser.document_reader import read_document
from ..brd_parser.requirement_extractor import RequirementExtractor
from ..brd_parser.hybrid_extractor import HybridExtractor
from ..brd_parser.spec_validator import validate_spec
from ..file_prep.gcs_uploader import GcsUploader
from ..file_prep.source_profiler import profile_table
from ..file_prep.xlsx_to_csv import XlsxToCsvConverter, validate_csv_outputs
from ..models.pipeline_spec import PipelineSpec
from ..models.validation import ValidationReport
from ..sql_generator import generate_00_sql, generate_01_sql, generate_load_sql
from ..sql_generator.render_template import SqlRenderer, write_sql
from ..utils.config import AppConfig, PROJECT_ROOT, load_config
from ..utils.logging_setup import get_logger, setup_logging

logger = get_logger(__name__)


@dataclass
class RunOptions:
    brd_path: str
    config_path: str = "configs/dev.yaml"
    run_id: str | None = None
    dry_run: bool = True
    source_root: str | None = None
    spec_path: str | None = None
    approval_token: str | None = None
    require_approval: bool = False
    non_interactive: bool = False
    pipeline_type: str = "GCS->00->01"
    # rule_based | llm | hybrid; None -> whatever configs/<env>.yaml says.
    extraction_mode: str | None = None

    generate_sql_only: bool = False
    prepare_files: bool = False
    upload_to_gcs: bool = False
    create_tables: bool = False
    load_01: bool = False
    validate: bool = False

    @property
    def stages(self) -> set[str]:
        selected = {
            "generate_sql": self.generate_sql_only,
            "prepare_files": self.prepare_files,
            "upload": self.upload_to_gcs,
            "create_tables": self.create_tables,
            "load_01": self.load_01,
            "validate": self.validate,
        }
        chosen = {name for name, on in selected.items() if on}
        return chosen or set(selected)  # nothing chosen -> run everything


class PipelineRunner:
    """Drives parse -> validate -> approve -> prepare -> upload -> create -> load -> validate."""

    def __init__(self, options: RunOptions, config: AppConfig | None = None) -> None:
        self.options = options
        self.config = config or load_config(options.config_path)
        self.dry_run = options.dry_run if options.dry_run is not None else self.config.safety.dry_run

        setup_logging(self.config.logging.level)
        if self.dry_run:
            logger.info("DRY RUN is ON - no data will be written to GCS or BigQuery.")
        else:
            logger.warning("DRY RUN is OFF - this run will write to GCS and BigQuery.")

        self.approval = ApprovalGate(
            config=self.config,
            dry_run=self.dry_run,
            interactive=not options.non_interactive,
            supplied_token=options.approval_token,
        )
        self.renderer = SqlRenderer(self.config.paths.templates_dir)
        self.report = ExecutionReport(
            run_id=options.run_id or "unknown",
            environment=self.config.environment,
            dry_run=self.dry_run,
        )
        self.spec: PipelineSpec | None = None
        self.executor: BigQueryExecutor | None = None
        self.sql_by_table: dict[str, dict[str, str]] = {}
        self.csv_by_table: dict[str, list[Path]] = {}
        self.extractor: HybridExtractor | None = None

    # -- stages -------------------------------------------------------------
    def parse_brd(self) -> PipelineSpec:
        if self.options.spec_path:
            spec = PipelineSpec.load(self.options.spec_path)
            self.report.step("parse_brd", "SKIPPED", reason=f"loaded spec from {self.options.spec_path}")
        else:
            document = read_document(self.options.brd_path)
            self.report.step("parse_brd", "OK", lines=len(document.lines), path=str(document.path))
            self.extractor = HybridExtractor(
                self.config,
                pipeline_type=self.options.pipeline_type,
                mode=self.options.extraction_mode,
            )
            result = self.extractor.extract(document, run_id=self.options.run_id)
            spec = result.spec
            self.report.extraction = result.to_dict()
            self.report.step(
                "extract_spec", "OK",
                strategy=result.strategy,
                provider=self.extractor.provider.name,
                tables=len(spec.tables),
            )
            self._report_low_confidence()

        self.spec = spec
        self.report.run_id = spec.run_id
        spec_path = spec.save(PROJECT_ROOT / self.config.paths.generated_specs_dir)
        self.report.attach_spec(spec, spec_path)
        return spec

    def validate_spec_stage(self) -> ValidationReport:
        report = validate_spec(self._spec(), self.config)
        self.report.attach_validations("spec", report)
        report.raise_if_failed("Pipeline spec validation")
        return report

    def approval_gate(self, action: str, destructive: bool = False) -> None:
        summary = summarize_spec(self._spec())
        summary["dry_run"] = self.dry_run
        if review := self._low_confidence():
            summary["llm_decisions_needing_review"] = review
        decision = self.approval.require(action, summary, destructive=destructive)
        self.report.approvals.append(decision.to_dict())

    def _low_confidence(self) -> list[dict[str, Any]]:
        """LLM judgement calls the reviewer should look at before approving."""
        if self.extractor is None:
            return []
        return [d.to_dict() for d in self.extractor.decision_agent.needs_review]

    def _report_low_confidence(self) -> None:
        for decision in self._low_confidence():
            logger.warning(
                "LOW CONFIDENCE (%.2f): %s -> %r",
                decision["confidence"], decision["question"], decision["answer"],
            )

    def generate_sql(self) -> dict[str, dict[str, str]]:
        spec = self._spec()
        sql_dir = PROJECT_ROOT / self.config.paths.generated_sql_dir
        for table in spec.tables:
            sql_00 = generate_00_sql(spec, table, self.config, self.renderer)
            sql_01 = generate_01_sql(spec, table, self.config, self.renderer)
            sql_load = generate_load_sql(spec, table, self.config, self.renderer)

            paths = [
                write_sql(sql_00, sql_dir / "00", f"{table.table_00}.sql"),
                write_sql(sql_01, sql_dir / "01", f"{table.table_01}.sql"),
                write_sql(sql_load, sql_dir / "load", f"load_{table.table_01}_{spec.run_id}.sql"),
            ]
            self.sql_by_table[table.source_name] = {"00": sql_00, "01": sql_01, "load": sql_load}
            self.report.generated_sql.extend(str(p.relative_to(PROJECT_ROOT)) for p in paths)

        self.report.step("generate_sql", "OK", files=len(self.report.generated_sql))
        return self.sql_by_table

    def prepare_files(self) -> ValidationReport:
        spec = self._spec()
        source_root = self.options.source_root
        report = ValidationReport()
        if not source_root:
            self.report.step("prepare_xlsx_to_csv", "SKIPPED", reason="--source-root not supplied")
            return report

        converter = XlsxToCsvConverter(self.config, dry_run=self.dry_run)
        for table in spec.tables:
            results = converter.convert_table(spec, table, source_root)
            self.csv_by_table[table.source_name] = [
                r.csv_path for r in results if r.csv_path is not None
            ]
            validate_csv_outputs(table, results, report, self.dry_run)
            self.report.step(
                f"prepare_{table.table_00}", "OK" if results else "SKIPPED",
                files=len(results), rows=sum(r.row_count for r in results),
            )
        self.report.attach_validations("files", report)
        return report

    def upload_to_gcs(self) -> None:
        spec = self._spec()
        if not self.csv_by_table:
            self.report.step("upload_csv_to_gcs", "SKIPPED", reason="no prepared CSVs")
            return

        self.approval_gate("upload_csv_to_gcs")
        uploader = GcsUploader(self.config, dry_run=self.dry_run)
        for table in spec.tables:
            files = self.csv_by_table.get(table.source_name, [])
            if not files:
                continue
            uploader.upload_table_files(spec, table, files)
        self.report.gcs_uploads.extend(uploader.audit)
        self.report.step("upload_csv_to_gcs", "OK", objects=len(uploader.audit))

    def profile_sources(self) -> dict[str, Any]:
        """Inspect the CSVs actually staged in GCS and record their real layout.

        The BRD states the contract; the staged files do not always honour it.
        Profiling keeps the 00 table faithful to the file so the 00 -> 01 load
        can normalise, instead of mapping declared names on positionally.
        """
        spec = self._spec()
        if self.dry_run:
            self.report.step("profile_sources", "SKIPPED", reason="dry run")
            return {}

        uploader = GcsUploader(self.config, dry_run=False)
        client = uploader.client()
        profiles: dict[str, Any] = {}
        for table in spec.tables:
            profile = profile_table(client, spec.gcs_bucket, table)
            if profile is None:
                self.report.step(f"profile_{table.table_00}", "SKIPPED", reason="no staged CSV")
                continue
            table.source_headers = profile.headers
            table.source_map = profile.mapping
            table.source_missing = profile.missing
            table.source_uri = f"gs://{spec.gcs_bucket}/{table.gcs_folder}/*.csv"
            if profile.missing:
                self._llm_rescue_headers(table, profile)
            profiles[table.source_name] = profile.to_dict()
            self.report.step(
                f"profile_{table.table_00}", "OK" if profile.conformant else "WARN",
                **profile.to_dict(),
            )
        self.report.source_profiles = profiles
        return profiles

    def _llm_rescue_headers(self, table, profile) -> None:
        """Ask the model to match headers that deterministic normalisation missed.

        Only unmatched spec columns and unclaimed headers are offered, so the
        model can add mappings but never overwrite one the rules were sure of.
        """
        if self.extractor is None or not self.config.llm.assist_header_mapping:
            return
        agent = self.extractor.decision_agent
        if not agent.enabled:
            return

        claimed = set(profile.mapping.values())
        free_headers = [h for h in profile.headers if h not in claimed]
        if not free_headers:
            return

        wanted = [
            {"name": c.name, "type": c.type, "description": c.description}
            for c in table.columns_00
            if c.name in profile.missing
        ]
        if not wanted:
            return

        decision = agent.map_headers(table.source_name, wanted, free_headers)
        extra = (decision.answer or {}).get("mapping") or {}
        recovered = {k: v for k, v in extra.items() if k in profile.missing and v in free_headers}
        if not recovered:
            return

        profile.mapping.update(recovered)
        profile.missing = [c for c in profile.missing if c not in recovered]
        table.source_map = profile.mapping
        table.source_missing = profile.missing
        logger.info(
            "LLM recovered %s header mapping(s) for %s: %s",
            len(recovered), table.source_name, recovered,
        )

    def create_tables(self) -> None:
        spec = self._spec()
        if not self.sql_by_table:
            self.generate_sql()

        self.approval_gate("create_00_and_01_tables")
        executor = self._executor()
        creator = TableCreator(executor)
        creator.ensure_datasets(spec)
        for table in spec.tables:
            sql = self.sql_by_table[table.source_name]
            creator.create_00(table, sql["00"])
            creator.create_01(table, sql["01"])
        self.report.step("create_tables", "OK", tables=len(spec.tables) * 2)

    def load_01_tables(self) -> None:
        spec = self._spec()
        if not self.sql_by_table:
            self.generate_sql()

        destructive = spec.rerun_strategy in ("replace_run", "truncate_table")
        self.approval_gate("load_01_replace_run", destructive=destructive)

        executor = self._executor()
        # Approval just happened; unlock DELETE for this executor only.
        executor.allow_destructive = self.config.safety.allow_destructive or not self.dry_run
        runner = LoadRunner(executor)
        summaries: list[dict[str, Any]] = []
        for table in spec.tables:
            summaries.append(runner.load(spec, table, self.sql_by_table[table.source_name]["load"]))
        self.report.step("load_01_replace_run", "OK", loads=summaries)

    def run_validations(self) -> ValidationReport:
        report = run_bigquery_validations(self._executor(), self._spec())
        self.report.attach_validations("bigquery", report)
        return report

    def publish_report(self) -> Path:
        if self.executor is not None:
            self.report.bigquery_jobs.extend(self.executor.audit)
        if self.extractor is not None:
            # Refresh: decisions are also made after parsing, during profiling.
            self.report.extraction["decisions"] = self.extractor.decision_agent.to_list()
        self.report.finish()
        return self.report.save(self.config.paths.generated_reports_dir)

    # -- full run -----------------------------------------------------------
    def run(self) -> ExecutionReport:
        stages = self.options.stages
        try:
            self.parse_brd()
            self.validate_spec_stage()

            if self.options.require_approval and not self.dry_run:
                self.approval_gate("execute_pipeline")

            if "generate_sql" in stages:
                self.generate_sql()
            if "prepare_files" in stages:
                self.prepare_files()
            if "upload" in stages:
                self.upload_to_gcs()
            if "create_tables" in stages or "load_01" in stages:
                # Profile after upload so newly staged files are included.
                self.profile_sources()
                self.sql_by_table.clear()
                self.generate_sql()
            if "create_tables" in stages:
                self.create_tables()
            if "load_01" in stages:
                self.load_01_tables()
            if "validate" in stages:
                self.run_validations()
        except ApprovalRejected as exc:
            self.report.add_error(f"Approval rejected: {exc}")
            self.report.finish("REJECTED")
        except Exception as exc:  # noqa: BLE001 - always publish a report
            self.report.add_error(f"{type(exc).__name__}: {exc}")
            logger.exception("Pipeline failed")
            self.report.finish("FAILED")
        finally:
            report_path = self.publish_report()
            logger.info("Run %s -> %s", self.report.status, report_path)
        return self.report

    # -- helpers ------------------------------------------------------------
    def _spec(self) -> PipelineSpec:
        if self.spec is None:
            raise RuntimeError("Pipeline spec is not loaded. Call parse_brd() first.")
        return self.spec

    def _executor(self) -> BigQueryExecutor:
        if self.executor is None:
            self.executor = BigQueryExecutor(
                config=self.config,
                dry_run=self.dry_run,
                allow_destructive=self.config.safety.allow_destructive,
            )
        return self.executor


# -- CLI --------------------------------------------------------------------
def _bool(value: str) -> bool:
    token = str(value).strip().lower()
    if token in {"true", "t", "yes", "y", "1"}:
        return True
    if token in {"false", "f", "no", "n", "0"}:
        return False
    raise argparse.ArgumentTypeError(f"Expected a boolean, got {value!r}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline_runner",
        description="Generate and optionally execute GCS -> 00 -> 01 BigQuery artifacts from a BRD.",
    )
    parser.add_argument("--brd-path", help="Path to the BRD (.docx, .md or .txt).")
    parser.add_argument("--config", dest="config_path", default="configs/dev.yaml")
    parser.add_argument("--run-id", help="Run identifier, e.g. 2026_04. Defaults to the latest historical load in the BRD.")
    parser.add_argument("--dry-run", type=_bool, default=True, help="Default true. Set false to write to GCS/BigQuery.")
    parser.add_argument("--source-root", help="Folder holding the source .xlsx files.")
    parser.add_argument("--spec-path", help="Skip BRD parsing and load an existing pipeline spec JSON.")
    parser.add_argument("--approval-token", help="Approval token (also read from BRD_AGENT_APPROVAL_TOKEN).")
    parser.add_argument("--require-approval", action="store_true", help="Gate the whole run behind approval.")
    parser.add_argument("--non-interactive", action="store_true", help="Never prompt; require a token instead.")
    parser.add_argument("--pipeline-type", default="GCS->00->01")
    parser.add_argument(
        "--extraction-mode",
        choices=["rule_based", "llm", "hybrid"],
        help="How to read the BRD. Defaults to the llm.extraction_mode in the config.",
    )

    stages = parser.add_argument_group("stages (choose any; default is all)")
    stages.add_argument("--generate-sql-only", action="store_true")
    stages.add_argument("--prepare-files", action="store_true")
    stages.add_argument("--upload-to-gcs", action="store_true")
    stages.add_argument("--create-tables", action="store_true")
    stages.add_argument("--load-01", action="store_true")
    stages.add_argument("--validate", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.brd_path and not args.spec_path:
        build_parser().error("one of --brd-path or --spec-path is required")

    options = RunOptions(
        brd_path=args.brd_path,
        config_path=args.config_path,
        run_id=args.run_id,
        dry_run=args.dry_run,
        source_root=args.source_root,
        spec_path=args.spec_path,
        approval_token=args.approval_token,
        require_approval=args.require_approval,
        non_interactive=args.non_interactive,
        pipeline_type=args.pipeline_type,
        extraction_mode=args.extraction_mode,
        generate_sql_only=args.generate_sql_only,
        prepare_files=args.prepare_files,
        upload_to_gcs=args.upload_to_gcs,
        create_tables=args.create_tables,
        load_01=args.load_01,
        validate=args.validate,
    )
    report = PipelineRunner(options).run()
    print(f"\nRun status: {report.status}")
    return 0 if report.status in ("SUCCESS", "COMPLETED_WITH_FAILURES") else 1


if __name__ == "__main__":
    sys.exit(main())
