"""Local dry-run CLI for the Regional Rainfall & Flood Watch BRD."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from ..brd_parser.document_reader import read_document
from ..llm.provider import LLMError, LLMRequestError, get_provider
from ..sql_generator.render_template import SqlRenderer
from ..utils.config import PROJECT_ROOT
from ..utils.logging_setup import setup_logging
from .llm_analysis import analyze_rainfall_brd
from .models import RainfallPipelineSpec
from .processing import (
    InventoryItem,
    default_source_root,
    discover_source_files,
    extract_pipeline_contract,
    inspect_sources,
    make_run_identity,
    source_inventory_markdown,
)
from .sql import generate_00_sql, plan_schema_evolution, write_00_sql

logger = logging.getLogger(__name__)

DEFAULT_BRD = Path(
    os.environ.get("BRD_INPUT_DIR")
    or PROJECT_ROOT.parent / "BRD_auto_dummy_files"
) / "Regional_Rainfall_Flood_Watch_Project_BRD.docx"
TEMPLATE_VERSION = "rainfall_00_v1"


def run_dry_run(
    *,
    brd_path: str | Path = DEFAULT_BRD,
    source_root: str | Path | None = None,
    run_date: date | None = None,
    output_root: str | Path | None = None,
    existing_schema_path: str | Path | None = None,
    environ: dict[str, str] | None = None,
    require_llm: bool = False,
) -> dict[str, Any]:
    """Run local extraction, profiling, validation, and plan generation only.

    This workflow never creates clients, connects to GCP, uploads objects, or
    executes SQL. Cloud mutation remains behind the existing human approval
    gate and is intentionally not part of this new BRD's dry-run command.
    """
    environment = os.environ if environ is None else environ
    source_dir = Path(source_root) if source_root else default_source_root(brd_path)
    sources = discover_source_files(source_dir)
    brd_sha = _sha256_file(brd_path)
    target_run_date = run_date or datetime.now(timezone.utc).date()
    run_id, idempotency_key, source_manifest = make_run_identity(
        brd_sha, target_run_date, sources
    )
    output_dir = Path(output_root) if output_root else PROJECT_ROOT / "generated"
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    reports_dir = output_dir / "reports"
    specs_dir = output_dir / "specs"
    sql_dir = output_dir / "sql" / "00"
    staging_dir = output_dir / "staging_csv"
    for directory in (reports_dir, specs_dir):
        directory.mkdir(parents=True, exist_ok=True)

    manifest_path = reports_dir / f"rainfall_run_{run_id}_manifest.json"
    ingested_at = _stable_ingestion_time(manifest_path, idempotency_key)

    llm_provider = _optional_value(environment, "LLM_PROVIDER")
    llm_model = _optional_value(environment, "LLM_MODEL")
    llm_key_configured = any(
        _optional_value(environment, name)
        for name in (
            "LLM_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
            "GOOGLE_API_KEY", "GEMINI_API_KEY", "LITELLM_API_KEY",
        )
    )
    project_id = _optional_value(environment, "GOOGLE_CLOUD_PROJECT")
    location = _optional_value(environment, "GOOGLE_CLOUD_LOCATION")
    bucket = _optional_value(environment, "GCS_LANDING_BUCKET")
    approved_dataset = _optional_value(environment, "BQ_DATASET_00")

    spec, proposed_dataset = extract_pipeline_contract(
        brd_path,
        run_date=target_run_date,
        run_id=run_id,
        idempotency_key=idempotency_key,
        ingested_at_utc=ingested_at,
        project_id=project_id,
        location=location,
        gcs_bucket=bucket,
        dataset_00=approved_dataset,
        dataset_00_is_approved=approved_dataset is not None,
        llm_provider=llm_provider,
        llm_model=llm_model,
        llm_credential_configured=llm_key_configured,
    )

    prepared = inspect_sources(
        sources,
        spec.feeds,
        staging_dir,
        run_id,
        target_run_date,
        ingested_at,
        bucket,
    )
    inventory_items = [source.item for source in prepared]
    llm_review = _run_llm_review(
        brd_path=brd_path,
        provider_name=llm_provider,
        model_name=llm_model,
        environment=environment,
        spec=spec,
        inventory=inventory_items,
    )
    llm_complete = llm_review["status"] == "COMPLETE"
    llm_analysis_path = reports_dir / f"rainfall_llm_analysis_{run_id}.json"
    _write_json(llm_analysis_path, llm_review)
    existing_schemas = _load_existing_schemas(existing_schema_path)
    schema_plans = {
        feed.table_00: plan_schema_evolution(
            existing_schemas.get(feed.table_00),
            feed,
            project_id=project_id,
            dataset_id=spec.dataset_00 if approved_dataset else None,
        )
        for feed in spec.feeds
    }
    sql_files: list[str] = []
    target_is_configured = all((project_id, location, bucket, approved_dataset))
    if target_is_configured and (not require_llm or llm_complete):
        for feed in spec.feeds:
            plan = schema_plans[feed.table_00]
            feed_sources = [
                item for item in prepared
                if item.feed_id == feed.feed_id and item.item.load_eligible
            ]
            if plan["action"] == "QUARANTINE" or not feed_sources:
                continue
            if any(source.item.accepted_rows == 0 for source in feed_sources):
                continue
            rendered = generate_00_sql(
                spec, feed, prepared, SqlRenderer(), schema_plan=plan
            )
            output = write_00_sql(rendered, feed, sql_dir)
            sql_files.append(str(output))

    inventory_path = reports_dir / f"rainfall_source_inventory_{run_id}.json"
    inventory_md_path = reports_dir / f"rainfall_source_inventory_{run_id}.md"
    _write_json(inventory_path, [item.to_dict() for item in inventory_items])
    inventory_md_path.write_text(source_inventory_markdown(inventory_items), encoding="utf-8")

    spec_path = specs_dir / f"rainfall_pipeline_spec_{run_id}.json"
    _write_json(spec_path, spec.model_dump(mode="json"))
    _write_json(
        reports_dir / f"rainfall_schema_evolution_{run_id}.json",
        list(schema_plans.values()),
    )
    schema_md = _schema_plan_markdown(list(schema_plans.values()))
    (reports_dir / f"rainfall_schema_evolution_{run_id}.md").write_text(
        schema_md, encoding="utf-8"
    )

    assessment = implementation_assessment()
    _write_json(reports_dir / "rainfall_implementation_assessment.json", assessment)
    (reports_dir / "rainfall_implementation_assessment.md").write_text(
        _assessment_markdown(assessment), encoding="utf-8"
    )
    one_layer_draft = _one_layer_draft(spec, environment)
    one_layer_json = reports_dir / f"rainfall_01_design_draft_{run_id}.json"
    one_layer_md = reports_dir / f"rainfall_01_design_draft_{run_id}.md"
    _write_json(one_layer_json, one_layer_draft)
    one_layer_md.write_text(_one_layer_markdown(one_layer_draft), encoding="utf-8")

    missing_config = [
        key
        for key, value in (
            ("GOOGLE_CLOUD_PROJECT", project_id),
            ("GOOGLE_CLOUD_LOCATION", location),
            ("GCS_LANDING_BUCKET", bucket),
            ("BQ_DATASET_00", approved_dataset),
        )
        if not value
    ]
    feed_ids_with_sources = {item.matched_feed for item in inventory_items if item.matched_feed}
    unresolved = [
        {
            "decision": "approved_cloud_target",
            "blocking": True,
            "question": (
                "Configure the approved DEV project, region, GCS bucket, and 00 dataset "
                "before generating executable target SQL or considering a cloud run."
            ),
            "missing_environment": missing_config,
        }
    ] if missing_config else []
    unresolved.extend(
        {
            "decision": f"source_file_for_{feed.feed_id}",
            "blocking": True,
            "question": f"No matching source file was found for {feed.source_name}.",
        }
        for feed in spec.feeds
        if feed.feed_id not in feed_ids_with_sources
    )
    unresolved.extend(one_layer_draft["blocking_questions"])
    if require_llm and not llm_complete:
        unresolved.append({
            "decision": "required_llm_review",
            "blocking": True,
            "question": llm_review["reason"],
        })
    warnings = [
        f"{item.source_file}: {reason}"
        for item in inventory_items
        for reason in item.quarantine_reasons
    ]
    if llm_review["status"] in {"FAILED", "UNAVAILABLE"}:
        warnings.append(llm_review["reason"])
    warnings.extend(
        issue.reason
        for item in inventory_items
        for issue in item.issues
        if issue.severity == "WARN"
    )
    all_eligible = all(
        any(item.matched_feed == feed.feed_id and item.load_eligible for item in inventory_items)
        for feed in spec.feeds
    )
    row_summary = {
        feed.feed_id: {
            "files": sum(1 for item in inventory_items if item.matched_feed == feed.feed_id),
            "accepted_rows": sum(
                item.accepted_rows for item in inventory_items
                if item.matched_feed == feed.feed_id
            ),
            "rejected_rows": sum(
                item.rejected_rows for item in inventory_items
                if item.matched_feed == feed.feed_id
            ),
        }
        for feed in spec.feeds
    }
    report_status = (
        "DRY_RUN_READY_FOR_APPROVAL"
        if target_is_configured and all_eligible and not missing_config
        else "DRY_RUN_COMPLETE_CONFIGURATION_REQUIRED"
    )
    if require_llm and not llm_complete:
        report_status = "DRY_RUN_BLOCKED_LLM_REVIEW"
    schema_alterations = [
        plan for plan in schema_plans.values()
        if plan["action"] in {"ADD_NULLABLE_COLUMNS", "QUARANTINE"}
    ]
    proposed_objects = [
        {
            "source_file_name": source.item.source_file,
            "feed": source.feed_id or None,
            "gcs_uri": source.proposed_object_uri,
            "sha256": source.item.sha256,
            "eligible": source.item.load_eligible,
        }
        for source in prepared
    ]
    run_manifest = {
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "run_date": target_run_date.isoformat(),
        "ingested_at_utc": ingested_at,
        "brd_file": Path(brd_path).name,
        "brd_sha256": brd_sha,
        "sources": source_manifest,
        "template_version": TEMPLATE_VERSION,
        "execution_status": "DRY_RUN_ONLY",
    }
    _write_json(manifest_path, run_manifest)

    proposed_project = project_id or "<approved-project-id>"
    approval_summary = {
        "brd_filename": Path(brd_path).name,
        "brd_sha256": brd_sha,
        "environment": "dev",
        "llm_model": llm_model,
        "llm_model_used": llm_review.get("model"),
        "llm_analysis_status": llm_review["status"],
        "llm_findings": llm_review["findings"],
        "llm_api_key_configured": llm_key_configured,
        "source_files": proposed_objects,
        "matched_feeds": sorted(feed_ids_with_sources),
        "proposed_gcs_object_paths": [item["gcs_uri"] for item in proposed_objects if item["gcs_uri"]],
        "proposed_00_tables": [
            f"{proposed_project}.{spec.dataset_00}.{feed.table_00}"
            for feed in spec.feeds
        ],
        "proposed_schema_alterations": schema_alterations,
        "proposed_01_tables": one_layer_draft["draft_tables"],
        "sql_files": sql_files,
        "affected_run_date": target_run_date.isoformat(),
        "row_counts": row_summary,
        "validations": {
            "source_files": len(inventory_items),
            "source_rows": sum(item.row_count for item in inventory_items),
            "accepted_rows": sum(item.accepted_rows for item in inventory_items),
            "rejected_rows": sum(item.rejected_rows for item in inventory_items),
            "all_three_feeds_matched": all_eligible,
        },
        "warnings": warnings,
        "quarantined_items": [
            {
                "source_file": item.source_file,
                "feed": item.matched_feed,
                "reasons": item.quarantine_reasons,
            }
            for item in inventory_items
            if not item.load_eligible or item.rejected_rows
        ],
        "unresolved_decisions": unresolved,
        "human_approval_required_before": [
            "GCS upload or overwrite",
            "BigQuery table creation or schema alteration",
            "BigQuery data loading or rerun",
            "DEV-to-QA/PROD promotion",
            "any 01-layer design execution",
        ],
        "approval_token": "APPROVE_DEV",
        "cloud_operations_performed": False,
    }
    approval_json = reports_dir / f"rainfall_approval_summary_{run_id}.json"
    approval_md = reports_dir / f"rainfall_approval_summary_{run_id}.md"
    _write_json(approval_json, approval_summary)
    approval_md.write_text(_approval_markdown(approval_summary), encoding="utf-8")

    report = {
        "status": report_status,
        "dry_run": True,
        "cloud_operations_performed": False,
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "run_date": target_run_date.isoformat(),
        "ingested_at_utc": ingested_at,
        "brd_file": Path(brd_path).name,
        "brd_sha256": brd_sha,
        "environment": "dev",
        "resource_configuration": {
            "project_id_configured": bool(project_id),
            "location_configured": bool(location),
            "gcs_bucket_configured": bool(bucket),
            "dataset_00_configured": bool(approved_dataset),
            "dataset_00_proposed_by_brd": proposed_dataset,
        },
        "llm": {
            "provider": llm_provider,
            "model": llm_model,
            "credential_configured": llm_key_configured,
            "status": llm_review["status"],
            "model_used": llm_review.get("model"),
            "summary": llm_review.get("summary"),
            "findings": llm_review["findings"],
            "error_type": llm_review.get("error_type"),
            "required": require_llm,
            "use": llm_review.get("reason", "Evidence-validated advisory reasoning; no cloud execution authority."),
        },
        "source_inventory_json": str(inventory_path),
        "source_inventory_markdown": str(inventory_md_path),
        "spec_path": str(spec_path),
        "schema_evolution": list(schema_plans.values()),
        "sql_files": sql_files,
        "row_counts": row_summary,
        "actions": {
            "would_create": [
                feed.table_00 for feed in spec.feeds
                if schema_plans[feed.table_00]["action"] == "CREATE_TABLE"
            ],
            "would_alter": [
                plan["table"] for plan in schema_alterations
                if plan["action"] == "ADD_NULLABLE_COLUMNS"
            ],
            "would_load": {
                feed_id: values["accepted_rows"]
                for feed_id, values in row_summary.items()
                if values["accepted_rows"] and any(
                    item.matched_feed == feed_id and item.load_eligible
                    for item in inventory_items
                )
            },
            "skipped_already_processed": "not checked; cloud manifest inspection is not performed in dry-run mode",
            "would_quarantine": approval_summary["quarantined_items"],
            "requires_approval": approval_summary["human_approval_required_before"],
        },
        "warnings": warnings,
        "unresolved_decisions": unresolved,
        "artifacts": {
            "implementation_assessment": str(reports_dir / "rainfall_implementation_assessment.md"),
            "01_design_draft": str(one_layer_md),
            "approval_summary": str(approval_md),
            "run_manifest": str(manifest_path),
            "llm_analysis": str(llm_analysis_path),
        },
    }
    report_json = reports_dir / f"rainfall_run_report_{run_id}.json"
    report_md = reports_dir / f"rainfall_run_report_{run_id}.md"
    _write_json(report_json, report)
    report_md.write_text(_run_markdown(report, inventory_items), encoding="utf-8")
    report["run_report_json"] = str(report_json)
    report["run_report_markdown"] = str(report_md)
    return report


def implementation_assessment() -> list[dict[str, str]]:
    """Capture the requested before-change assessment in the generated report."""
    return [
        {
            "capability": "BRD parsing",
            "reusable_component": "src/brd_parser/document_reader.py reads DOCX paragraphs and tables.",
            "gap": "Existing requirement extractor expects a WFRS Development Location block and 00/01 pairing.",
            "required_code_change": "Add deterministic rainfall schema-table extraction and strict Pydantic contracts.",
            "test_needed": "Parse the actual BRD and assert exactly three contracts, fields, types, keys, partitioning, and clustering.",
            "execution_risk": "A malformed or unclassifiable BRD table must fail extraction, not fall back to legacy targets.",
            "human_approval": "Approve cloud target configuration and any unresolved interpretation.",
        },
        {
            "capability": "LLM integration",
            "reusable_component": "src/llm/provider.py and existing structured extraction/decision patterns.",
            "gap": "Legacy LLM output is the 00-to-01 PipelineSpec; it is not the rainfall 00 contract.",
            "required_code_change": "Keep LLM optional, read credentials only from environment, and use deterministic extraction where the BRD is explicit.",
            "test_needed": "Verify credential status is boolean-only and missing credentials do not prevent local deterministic runs.",
            "execution_risk": "No key is configured; no model call or unverified LLM-derived deployment is permitted.",
            "human_approval": "Review any future LLM-derived model or transformation before use.",
        },
        {
            "capability": "Pipeline specification",
            "reusable_component": "Pydantic v2 models under src/models with extra='forbid'.",
            "gap": "Existing PipelineSpec requires 01 tables and source metadata columns.",
            "required_code_change": "Add a separate strict rainfall spec with source-only schema and explicit metadata.",
            "test_needed": "Reject unknown fields, invalid identifiers/types, unsupported partition/cluster fields, and metadata in source schemas.",
            "execution_risk": "Mixing rainfall schemas with the legacy spec can generate invalid SQL.",
            "human_approval": "Approve schema additions and the separate 00 staging/managed-table pattern.",
        },
        {
            "capability": "00 SQL templates",
            "reusable_component": "Jinja StrictUndefined renderer and generated/sql/00 naming convention.",
            "gap": "Existing 00 template is an external table only and cannot add metadata absent from CSV.",
            "required_code_change": "Add an external staging table plus typed managed 00 table and metadata-populating load transaction.",
            "test_needed": "Render all three DDL/load scripts and assert no unresolved placeholders or metadata/source mismatch.",
            "execution_risk": "This is an architectural variation from legacy external-only 00; staging tables persist.",
            "human_approval": "Approval is required before applying managed-table DDL or loading.",
        },
        {
            "capability": "GCS and BigQuery",
            "reusable_component": "Existing ADC/impersonation clients and approval-aware execution wrappers.",
            "gap": "Legacy dev.yaml pins Wildfire project/bucket and is not approved for this BRD.",
            "required_code_change": "Resolve rainfall destinations only from approved environment variables; dry-run makes no cloud calls.",
            "test_needed": "Assert absent target settings block SQL generation and all local dry-run artifacts still complete.",
            "execution_risk": "Project, location, bucket, and dataset are not configured; no cloud operation can proceed.",
            "human_approval": "Human approval and explicit token required before any cloud mutation.",
        },
        {
            "capability": "Approval gate",
            "reusable_component": "src/approval/approval_gate.py exact-token gate.",
            "gap": "The legacy summary describes 00/01 and a destructive run_date partition replacement.",
            "required_code_change": "Emit a rainfall-specific approval summary, but keep the new command plan-only.",
            "test_needed": "Check summary includes checksum, target names, sources, row counts, schema plan, run date, and open decisions.",
            "execution_risk": "No execution token was supplied; no resources are modified.",
            "human_approval": "Required for upload, create/alter, load, rerun, promotion, or 01 implementation.",
        },
        {
            "capability": "Validation and source profiling",
            "reusable_component": "Existing source profiler, CSV preparation, validation report types, and ignored staging dirs.",
            "gap": "No feed-specific checks for rainfall, effective-dated assignments, thresholds, or CSV-only source metadata.",
            "required_code_change": "Inventory CSV/XLSX inputs, strict-match headers, validate typed rows, and stage only accepted rows.",
            "test_needed": "Conforming files plus negative rainfall, invalid flags/dates, duplicate keys, interval overlap, and nullability cases.",
            "execution_risk": "Invalid records are quarantined; valid rows remain locally inspectable.",
            "human_approval": "Review quarantines and warnings before any load.",
        },
        {
            "capability": "CLI and orchestration",
            "reusable_component": "src/orchestrator/pipeline_runner.py and dags/brd_to_bigquery_pipeline.py for legacy WFRS.",
            "gap": "Legacy command and DAG rely on 01 tables and WFRS assumptions.",
            "required_code_change": "Add a distinct local rainfall dry-run CLI; do not redirect the old DAG.",
            "test_needed": "Local end-to-end run against the BRD and its three CSVs with no GCP credentials.",
            "execution_risk": "Production trigger, schedule, identity, and retention are unresolved.",
            "human_approval": "No DAG deployment until operational owners approve schedule and runtime design.",
        },
        {
            "capability": "Tests and generated artifacts",
            "reusable_component": "pytest suite and generated/{sql,specs,reports,staging_csv} layout.",
            "gap": "No rainfall acceptance tests; current generated artifacts include prior WFRS outputs.",
            "required_code_change": "Add focused rainfall tests and write new artifacts under rainfall-specific names.",
            "test_needed": "Unit, integration, and local end-to-end coverage with synthetic source values.",
            "execution_risk": "Generated artifacts are local and ignored; no prior outputs are overwritten.",
            "human_approval": "Review output reports and SQL before execution.",
        },
    ]


def _one_layer_draft(spec, environment: dict[str, str]) -> dict[str, Any]:
    draft_tables = [
        {
            "name": "stg_rainfall_source_feeds",
            "status": "DRAFT_REQUIRES_APPROVAL",
            "basis": "BRD 7.2 requires source-specific staging and traceable source-to-target mappings.",
        },
        {
            "name": "dim_station_or_equivalent",
            "status": "DRAFT_REQUIRES_APPROVAL",
            "basis": "BRD 7.2 requires conformed station references and effective-dated assignments.",
        },
        {
            "name": "dim_watershed_or_equivalent",
            "status": "DRAFT_REQUIRES_APPROVAL",
            "basis": "BRD 7.2 requires watershed reference entities and temporal joins.",
        },
        {
            "name": "fact_daily_station_rainfall",
            "status": "DRAFT_REQUIRES_APPROVAL",
            "basis": "BRD 7.2 defines one station per observation date.",
        },
        {
            "name": "fact_station_rolling_rainfall_assessment",
            "status": "DRAFT_REQUIRES_APPROVAL",
            "basis": "BRD 7.2 defines one station per assessment date and rolling window.",
        },
        {
            "name": "fact_watershed_station_coverage",
            "status": "DRAFT_REQUIRES_APPROVAL",
            "basis": "BRD 7.2 defines one watershed per assessment date.",
        },
    ]
    questions = [
        "Approve the 01 dataset name; BQ_DATASET_01 is not configured.",
        "Choose the physical 01 layer pattern (typed copy, deduplicated current view, history table, or analytical model).",
        "Approve 01 table names, business keys, and source-to-target mappings.",
        "Define deduplication rules and how duplicate source grains should be represented downstream.",
        "Approve 01 run_date/load timestamp conventions, partitioning, clustering, and rerun strategy.",
        "Decide whether rolling rainfall, watershed coverage, and threshold assessments belong in 01 or a later layer.",
        "Approve effective-date join and incomplete-window handling in downstream SQL.",
        "Confirm model ownership and qualified review for threshold interpretation.",
    ]
    return {
        "status": "DRAFT_REQUIRES_APPROVAL",
        "execution_permitted": False,
        "dataset_01": environment.get("BQ_DATASET_01") or None,
        "draft_tables": draft_tables,
        "lineage_requirements": [
            "Join observations to station assignments by station_id and observation date within effective dates.",
            "Join thresholds by watershed_id, rolling_window_days, and assessment date within valid dates.",
            "Preserve source keys, effective dates, source filename, run identity, and transformation lineage.",
        ],
        "business_rules": [
            "Rolling accumulation requires every expected calendar day to have a VALID or ESTIMATED observation.",
            "Insufficient station coverage or incomplete windows must be NOT_ASSESSED.",
            "Threshold categories are not forecasts or official warnings.",
            "Do not sum station rainfall and label it watershed rainfall.",
        ],
        "blocking_questions": [
            {
                "question": question,
                "required_before": "01 schema generation or execution",
            }
            for question in questions
        ],
        "source_00_tables": [feed.table_00 for feed in spec.feeds],
    }


def _stable_ingestion_time(manifest_path: Path, idempotency_key: str) -> str:
    if manifest_path.exists():
        previous = json.loads(manifest_path.read_text(encoding="utf-8"))
        if previous.get("idempotency_key") != idempotency_key:
            raise ValueError("Existing run manifest does not match the deterministic run identity.")
        timestamp = previous.get("ingested_at_utc")
        if not isinstance(timestamp, str) or not timestamp.endswith("Z"):
            raise ValueError("Existing run manifest has no valid UTC ingestion timestamp.")
        return timestamp
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _load_existing_schemas(path: str | Path | None) -> dict[str, list[dict[str, Any]]]:
    if path is None:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or any(
        not isinstance(table, str) or not isinstance(columns, list)
        for table, columns in payload.items()
    ):
        raise ValueError("Existing-schema JSON must map table names to column lists.")
    return payload


def _optional_value(environ: dict[str, str], name: str) -> str | None:
    value = environ.get(name, "").strip()
    if value.startswith("<") and value.endswith(">"):
        return None
    return value or None


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


def _schema_plan_markdown(plans: list[dict[str, Any]]) -> str:
    lines = [
        "# Rainfall 00 schema evolution plan",
        "",
        "No cloud schema inspection was performed. Existing-schema JSON may be supplied for a local comparison.",
        "",
        "| Table | Action | Existing schema checked | Additions | Historical columns preserved |",
        "| --- | --- | --- | --- | --- |",
    ]
    for plan in plans:
        lines.append(
            f"| {plan['table']} | {plan['action']} | {plan['existing_schema_checked']} | "
            f"{', '.join(item['name'] for item in plan['additions']) or '-'} | "
            f"{', '.join(plan['preserved_historical_columns']) or '-'} |"
        )
        lines.append(f"\n{plan['reason']}\n")
        for statement in plan.get("alter_statements", []):
            lines.append(f"\n```sql\n{statement}\n```\n")
    return "\n".join(lines) + "\n"


def _assessment_markdown(items: list[dict[str, str]]) -> str:
    lines = ["# Rainfall BRD implementation assessment", ""]
    for item in items:
        lines.extend(
            [
                f"## {item['capability']}",
                f"- **Existing capability:** {item['reusable_component']}",
                f"- **Gap:** {item['gap']}",
                f"- **Required code change:** {item['required_code_change']}",
                f"- **Test needed:** {item['test_needed']}",
                f"- **Execution risk:** {item['execution_risk']}",
                f"- **Human approval:** {item['human_approval']}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _one_layer_markdown(draft: dict[str, Any]) -> str:
    lines = [
        "# Proposed 01-layer design",
        "",
        f"**Status:** {draft['status']}  ",
        "**Execution:** prohibited until decisions are approved.",
        "",
        "## Draft models",
        "",
    ]
    for table in draft["draft_tables"]:
        lines.append(f"- **{table['name']}** — {table['status']}: {table['basis']}")
    lines.extend(["", "## Decisions required before implementation", ""])
    lines.extend(f"- {item['question']}" for item in draft["blocking_questions"])
    lines.extend(["", "## Grounded transformation rules", ""])
    lines.extend(f"- {rule}" for rule in draft["business_rules"])
    return "\n".join(lines) + "\n"


def _approval_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Rainfall dry-run approval summary",
        "",
        f"- **BRD:** {summary['brd_filename']}",
        f"- **BRD SHA-256:** `{summary['brd_sha256']}`",
        f"- **Environment:** {summary['environment']}",
        f"- **LLM model:** {summary['llm_model'] or 'not configured'}",
        f"- **LLM credential configured:** {summary['llm_api_key_configured']}",
        f"- **Run date:** {summary['affected_run_date']}",
        f"- **Approval token for a future DEV mutation:** `{summary['approval_token']}`",
        "",
        "## Proposed 00 tables",
        "",
    ]
    lines.extend(f"- `{table}`" for table in summary["proposed_00_tables"])
    lines.extend(["", "## Source files and proposed object paths", ""])
    for source in summary["source_files"]:
        lines.append(
            f"- `{source['source_file_name']}` → {source['gcs_uri'] or 'GCS bucket not configured'} "
            f"({source['feed'] or 'unmatched'}, eligible={source['eligible']})"
        )
    lines.extend(["", "## Row counts", ""])
    for feed, values in summary["row_counts"].items():
        lines.append(
            f"- {feed}: {values['files']} file(s), {values['accepted_rows']} accepted, "
            f"{values['rejected_rows']} rejected"
        )
    lines.extend(["", "## SQL files", ""])
    lines.extend(f"- `{path}`" for path in summary["sql_files"])
    if not summary["sql_files"]:
        lines.append("- No SQL generated because the approved cloud target is not configured or a feed is ineligible.")
    lines.extend(["", "## Required approvals", ""])
    lines.extend(f"- {action}" for action in summary["human_approval_required_before"])
    lines.extend(["", "## Unresolved decisions", ""])
    lines.extend(
        f"- {item.get('decision', '01_design')}: {item.get('question') or ', '.join(item.get('missing_environment', []))}"
        for item in summary["unresolved_decisions"]
    )
    return "\n".join(lines) + "\n"


def _run_llm_review(
    *,
    brd_path: str | Path,
    provider_name: str | None,
    model_name: str | None,
    environment: dict[str, str],
    spec: RainfallPipelineSpec,
    inventory: list[InventoryItem],
) -> dict[str, Any]:
    if not provider_name or not model_name:
        return {
            "status": "NOT_CONFIGURED",
            "reason": "Configure LLM_PROVIDER and LLM_MODEL before LLM reasoning.",
            "findings": [],
        }
    options = {}
    if provider_name == "litellm":
        options = {
            "api_key": (
                _optional_value(environment, "LLM_API_KEY")
                or _optional_value(environment, "LITELLM_API_KEY")
                or _optional_value(environment, "OPENAI_API_KEY")
            ),
            "api_base": (
                _optional_value(environment, "LLM_API_BASE")
                or _optional_value(environment, "OPENAI_API_BASE")
            ),
        }
        if not options["api_key"]:
            return {
                "status": "NOT_CONFIGURED",
                "reason": "Configure an LLM API credential before LLM reasoning.",
                "findings": [],
            }
    try:
        provider = get_provider(provider_name, model_name, **options)
        if not provider.available():
            return {
                "status": "UNAVAILABLE",
                "reason": "Configured LLM provider is unavailable; check its SDK and credential configuration.",
                "findings": [],
            }
        analysis, response = analyze_rainfall_brd(
            read_document(brd_path), spec, inventory, provider, max_attempts=2
        )
    except (LLMError, ValidationError, ValueError) as exc:
        logger.warning("LLM review failed (%s); sensitive exception details omitted.", type(exc).__name__)
        return {
            "status": "FAILED",
            "reason": "LLM request or deterministic evidence validation failed; review provider access and the BRD contract.",
            "error_type": exc.error_type if isinstance(exc, LLMRequestError) else type(exc).__name__,
            "http_status": exc.status_code if isinstance(exc, LLMRequestError) else None,
            "findings": [],
        }
    return {
        "status": "COMPLETE",
        "provider": response.provider,
        "model": response.model,
        "summary": analysis.summary,
        "findings": [finding.model_dump(mode="json") for finding in analysis.findings],
        "response_metadata": response.to_dict(),
    }


def _run_markdown(report: dict[str, Any], items: list[InventoryItem]) -> str:
    lines = [
        "# Rainfall BRD dry-run report",
        "",
        f"- **Status:** {report['status']}",
        f"- **Dry run:** {report['dry_run']}",
        f"- **Cloud operations performed:** {report['cloud_operations_performed']}",
        f"- **BRD:** {report['brd_file']}",
        f"- **BRD SHA-256:** `{report['brd_sha256']}`",
        f"- **Run date (UTC):** {report['run_date']}",
        f"- **Run identity:** `{report['run_id']}`",
        f"- **Idempotency key:** `{report['idempotency_key']}`",
        f"- **Ingestion timestamp (UTC):** `{report['ingested_at_utc']}`",
        f"- **LLM analysis:** {report['llm']['status']}",
        f"- **LLM model used:** {report['llm']['model_used'] or 'none'}",
        "",
        "## Configuration",
        "",
    ]
    lines.extend(
        f"- {name}: {'configured' if value else 'not configured'}"
        for name, value in report["resource_configuration"].items()
        if name.endswith("_configured")
    )
    lines.extend(["", "## Actions", ""])
    for action, values in report["actions"].items():
        lines.append(f"- **{action}:** {json.dumps(values, ensure_ascii=False)}")
    lines.extend(["", "## Source inventory", "", source_inventory_markdown(items)])
    lines.extend(["## Artifacts", ""])
    lines.extend(f"- `{value}`" for value in report["artifacts"].values())
    if report["warnings"]:
        lines.extend(["", "## Warnings and quarantines", ""])
        lines.extend(f"- {value}" for value in report["warnings"])
    if report["unresolved_decisions"]:
        lines.extend(["", "## Unresolved decisions", ""])
        for decision in report["unresolved_decisions"]:
            lines.append(
                f"- {decision.get('question', decision.get('decision'))}: "
                f"{', '.join(decision.get('missing_environment', []))}"
            )
    return "\n".join(lines).rstrip() + "\n"


def _parse_cli_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("run date must be YYYY-MM-DD") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rainfall BRD local dry-run planner (no GCP calls).")
    parser.add_argument("--brd-path", type=Path, default=DEFAULT_BRD)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--run-date", type=_parse_cli_date)
    parser.add_argument("--output-root", type=Path, help="Defaults to generated/ under the framework.")
    parser.add_argument("--existing-schema", type=Path, help="JSON map of table names to existing columns.")
    parser.add_argument("--require-llm", action="store_true", help="Block SQL planning unless live LLM reasoning passes validation.")
    args = parser.parse_args(argv)
    setup_logging()
    try:
        report = run_dry_run(
            brd_path=args.brd_path,
            source_root=args.source_root,
            run_date=args.run_date,
            output_root=args.output_root,
            existing_schema_path=args.existing_schema,
            require_llm=args.require_llm,
        )
    except (FileNotFoundError, ValueError, OSError) as exc:
        logger.error("Rainfall dry run stopped: %s", exc)
        return 2
    print(f"Rainfall dry-run status: {report['status']}")
    print(f"Run report: {report['run_report_markdown']}")
    print(f"LLM API credential configured: {'yes' if report['llm']['credential_configured'] else 'no'}")
    print(f"Generated SQL files: {len(report['sql_files'])}")
    print(f"LLM analysis status: {report['llm']['status']}")
    return 2 if report["status"] == "DRY_RUN_BLOCKED_LLM_REVIEW" else 0


if __name__ == "__main__":
    sys.exit(main())
