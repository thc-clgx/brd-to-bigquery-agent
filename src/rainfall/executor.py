"""Execute the validated rainfall contract with ADC and non-destructive DEV writes."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from google.api_core.exceptions import Conflict, GoogleAPICallError, NotFound, PreconditionFailed
from google.auth.exceptions import GoogleAuthError
from google.cloud import bigquery, storage

from ..brd_parser.document_reader import read_document
from ..llm.provider import LLMError, get_provider
from ..sql_generator.render_template import SqlRenderer, write_sql
from ..utils.config import PROJECT_ROOT
from .execution_plan import OPERATIONS, compile_transform_sql, propose_execution_plan
from .acceptance import verify_analytical_examples
from .preflight import authentication_guidance, development_resources, extract_curated_contracts, verify_workload_identity
from .processing import (
    discover_source_files, extract_pipeline_contract, inspect_sources, make_run_identity, sha256_file,
)

LABELS = {"application": "regional-rainfall-watch", "environment": "dev", "data_classification": "public"}
SOURCE_NAMES = {
    "rain_gauge_daily_observations": "regional_rainfall_daily_observations.csv",
    "watershed_station_map": "regional_watershed_station_map.csv",
    "flood_watch_thresholds": "regional_flood_watch_thresholds.csv",
}


class ContractBlocked(ValueError):
    """A deterministic contract/permission/data check stopped the run."""


def field_schema(column) -> bigquery.SchemaField:
    if column.type.startswith("ARRAY<"):
        return bigquery.SchemaField(
            column.name, column.type[6:-1], mode="REPEATED", description=column.definition,
        )
    return bigquery.SchemaField(
        column.name, column.type, mode="NULLABLE" if column.nullable else "REQUIRED",
        description=getattr(column, "description", None) or getattr(column, "definition", None),
    )


def schema_actions(
    existing: bigquery.Table, wanted: bigquery.Table, *, allow_nullable_additions: bool,
) -> list[bigquery.SchemaField]:
    if existing.table_type != "TABLE":
        raise ContractBlocked(f"{existing.table_id}: existing object is not a native managed table.")
    actual = {field.name: field for field in existing.schema}
    additions = []
    aliases = {"FLOAT": "FLOAT64", "INTEGER": "INT64", "BOOLEAN": "BOOL"}
    for field in wanted.schema:
        old = actual.get(field.name)
        if old is None:
            if field.mode != "NULLABLE" or not allow_nullable_additions:
                raise ContractBlocked(f"{existing.table_id}.{field.name}: unsupported schema addition.")
            additions.append(field)
        elif aliases.get(old.field_type, old.field_type) != aliases.get(field.field_type, field.field_type) or old.mode != field.mode:
            raise ContractBlocked(f"{existing.table_id}.{field.name}: type/nullability change is forbidden.")
    old_partition = existing.time_partitioning.field if existing.time_partitioning else None
    new_partition = wanted.time_partitioning.field if wanted.time_partitioning else None
    if old_partition != new_partition or (existing.clustering_fields or []) != (wanted.clustering_fields or []):
        raise ContractBlocked(f"{existing.table_id}: existing partitioning/clustering conflicts with BRD.")
    return additions


def object_path(feed_id: str, filename: str, run_date: date) -> str:
    if filename != SOURCE_NAMES[feed_id]:
        raise ContractBlocked("Source filename does not match the registered BRD feed.")
    if feed_id == "rain_gauge_daily_observations":
        return f"rainfall_observations/{run_date:%Y/%m/%d}/{filename}"
    return f"{feed_id}/snapshot_date={run_date.isoformat()}/{filename}"


def ensure_object(bucket, name: str, data: bytes, *, content_type: str) -> dict:
    checksum = hashlib.sha256(data).hexdigest()
    blob = bucket.blob(name)
    try:
        blob.upload_from_string(data, content_type=content_type, if_generation_match=0)
        action = "UPLOADED"
    except (PreconditionFailed, Conflict):
        blob.reload()
        existing = blob.download_as_bytes(if_generation_match=int(blob.generation))
        if hashlib.sha256(existing).hexdigest() != checksum:
            raise ContractBlocked(f"Existing GCS object has different content: {name}; overwrite blocked.") from None
        action = "REUSED_IDENTICAL"
    return {
        "uri": f"gs://{bucket.name}/{name}", "generation": str(blob.generation),
        "sha256": checksum, "action": action,
    }


def typed_record(row: dict[str, str], columns: list) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for column in columns:
        text = row[column.name]
        if text == "":
            result[column.name] = None
        elif column.type == "INT64":
            result[column.name] = int(text)
        elif column.type == "FLOAT64":
            value = float(text)
            if not math.isfinite(value):
                raise ContractBlocked(f"Source field {column.name} exceeds the finite FLOAT64 range.")
            result[column.name] = value
        elif column.type == "BOOL":
            result[column.name] = text.lower() == "true"
        elif column.type == "TIMESTAMP":
            result[column.name] = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        else:
            result[column.name] = text
    return result


def checked_job(client, job_id: str, submit):
    """Reuse a previous atomic load/query job instead of issuing duplicate writes."""
    try:
        job = client.get_job(job_id, location="US")
        reused = True
    except NotFound:
        job = submit()
        reused = False
    job.result()
    if job.error_result:
        raise ContractBlocked(f"BigQuery job {job_id} did not succeed.")
    return job, reused


def load_rows(
    client, bucket, obj: dict, table_id: str, feed_id: str, columns: list,
    rows: list[dict], run_date: date, ingested_at: datetime, brd_sha256: str,
    metadata: dict[str, str],
) -> dict:
    """Append one source generation atomically; receipts survive local-state loss."""
    load_id = hashlib.sha256(f"{obj['uri']}:{obj['generation']}:{run_date}".encode()).hexdigest()
    receipt_blob = bucket.blob(f"run_reports/load_receipts/{load_id}.json")
    try:
        receipt = json.loads(receipt_blob.download_as_text())
        receipt_generation = int(receipt_blob.generation)
    except NotFound:
        receipt = {
            "source": obj, "feed": feed_id, "status": "STARTED",
            "ingested_at": ingested_at.isoformat(), "run_date": run_date.isoformat(),
            "brd_sha256": brd_sha256, "accepted_rows": len(rows),
        }
        receipt_blob.upload_from_string(json.dumps(receipt), content_type="application/json", if_generation_match=0)
        receipt_generation = int(receipt_blob.generation)
    values = {
        "filename": obj["uri"].rsplit("/", 1)[-1],
        "ingested_at": receipt["ingested_at"], "run_date": receipt["run_date"],
    }
    for row in rows:
        row.update({name: values[generator] for name, generator in metadata.items()})
    payload = "\n".join(json.dumps(row, allow_nan=False) for row in rows).encode()
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[field_schema(column) for column in columns],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        max_bad_records=0, ignore_unknown_values=False, labels=LABELS,
    )
    job_id = f"brd_raw_{load_id}"
    job, reused = checked_job(client, job_id, lambda: client.load_table_from_file(
        io.BytesIO(payload), table_id, job_config=config, job_id=job_id, location="US",
    ))
    destination = job.destination
    if f"{destination.project}.{destination.dataset_id}.{destination.table_id}" != table_id:
        raise ContractBlocked("The source generation was already loaded to a different table; remapping is blocked.")
    receipt.update(status="COMPLETE", job_id=job.job_id)
    receipt_blob.upload_from_string(
        json.dumps(receipt), content_type="application/json", if_generation_match=receipt_generation,
    )
    return {"phase": "00", "table": table_id, "job_id": job.job_id, "reused": reused, "rows": job.output_rows}


def verify_unchanged_retry(client, sql: str, parameters: list, expected_merges: int) -> dict:
    """Re-execute the approved MERGE script and prove unchanged inputs cause zero writes."""
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=parameters, maximum_bytes_billed=1024**3, labels=LABELS,
    ), location="US")
    job.result()
    statistics = []
    for child in client.list_jobs(parent_job=job.job_id):
        if child.statement_type == "MERGE":
            stats = client.get_job(child.job_id, location="US").dml_stats
            statistics.append({
                "inserted": stats.inserted_row_count, "updated": stats.updated_row_count,
                "deleted": stats.deleted_row_count,
            })
    if len(statistics) != expected_merges or any(
        any(count != 0 for count in item.values()) for item in statistics
    ):
        raise ContractBlocked("Unchanged-input retry modified curated rows; idempotency verification failed.")
    return {"status": "PASS", "job_id": job.job_id, "merge_statistics": statistics}


def write_report(report: dict, directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    report["report_directory"] = str(directory)
    (directory / "run_report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    lines = [
        "# BRD execution report", "", f"**Status: {report['status']}**",
        "", f"BRD: {report.get('brd_file', '-')}", f"SHA-256: {report.get('brd_sha256', '-')}",
        "", "## LLM versus automatic execution", "",
        "| Step | Actor | Status | Detail |", "| --- | --- | --- | --- |",
    ]
    for step in report["steps"]:
        detail = str(step.get("detail", "")).replace("|", "/").replace("\n", " ")
        lines.append(f"| {step['step']} | {step['actor']} | {step['status']} | {detail} |")
    lines.extend([
        "", "## Traceback and corrective actions", "",
        "| Requirement/check | Result | Action needed |", "| --- | --- | --- |",
    ])
    for issue in report.get("issues", []):
        lines.append(f"| {issue['check']} | {issue['result']} | {issue['action']} |")
    if not report.get("issues"):
        lines.append("| Executed contract checks | PASS | None |")
    lines.extend(["", "## Verified row counts", "", "| Table | Rows |", "| --- | ---: |"])
    for name, count in report.get("row_counts", {}).items():
        lines.append(f"| {name} | {count} |")
    lines.extend([
        "", "## Execution scope", "",
        "This report describes a CLI batch execution. No deployed object-finalized trigger is claimed.",
        "Production release, operational threshold approval, and destructive reconciliation are outside this run.",
        "", "## Remaining BRD capability differences", "",
        "| Requirement | Current behavior | Resolution |",
        "| --- | --- | --- |",
        "| GCS object-finalized trigger | Not deployed; CLI batch orchestration | Deploy a separately configured event runtime |",
        "| Partial row-rejection publication | Batch blocks rather than publishing partial reference inputs | Add quarantine/complete-snapshot selection before enabling this mode |",
        "| Complete snapshot removing dimension keys | Blocks; never deletes previous dimension rows | Explicitly reviewed reconciliation required |",
    ])
    (directory / "run_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def execute(
    brd: Path, source_root: Path, output_root: Path, *, dry_run: bool, verify_retry: bool = False,
) -> dict:
    now = datetime.now(timezone.utc)
    report: dict[str, Any] = {
        "status": "RUNNING", "brd_file": brd.name, "steps": [], "issues": [], "jobs": [],
        "objects": [], "row_counts": {}, "cloud_mutations": [], "mode": "DRY_RUN" if dry_run else "DEV_EXECUTE",
    }
    directory = output_root / "reports" / f"blocked_{now:%Y%m%dT%H%M%S%f}"
    phase = "authentication"
    try:
        # Authentication is checked before parsing/LLM calls or resource writes.
        auth = verify_workload_identity()
        report["authentication"] = auth
        if auth["status"] != "PASS":
            raise ContractBlocked(authentication_guidance(auth))
        report["steps"].append({"step": phase, "actor": "Python/Google ADC", "status": "PASS"})
        phase = "BRD contract extraction"
        document = read_document(brd)
        resources = development_resources(document)
        configured_project = os.getenv("GOOGLE_CLOUD_PROJECT")
        if configured_project != resources["Google Cloud project ID"]:
            raise ContractBlocked(
                "GOOGLE_CLOUD_PROJECT must exactly match the BRD's authorized project; "
                "update the private environment file, then rerun."
            )
        target = resources.get("Pipeline service account")
        if target:
            raise ContractBlocked("This executor requires ADC; an explicit impersonation contract needs configuration.")
        curated = extract_curated_contracts(document)
        sources = discover_source_files(source_root)
        run_id, key, manifest = make_run_identity(sha256_file(brd), now.date(), sources)
        directory = output_root / "reports" / f"execution_{run_id}"
        report.update({
            "run_id": run_id, "idempotency_key": key, "brd_sha256": sha256_file(brd),
            "resources": resources, "source_manifest": manifest, "run_date": now.date().isoformat(),
            "report_directory": str(directory), "template_version": "rainfall_01_v2",
        })
        spec, _ = extract_pipeline_contract(
            brd, run_date=now.date(), run_id=run_id, idempotency_key=key, ingested_at_utc=now.isoformat(),
            project_id=resources["Google Cloud project ID"], location="US",
            gcs_bucket=resources["GCS landing bucket"], dataset_00=resources["00 dataset"],
            dataset_00_is_approved=True, llm_provider=os.getenv("LLM_PROVIDER"),
            llm_model=os.getenv("LLM_MODEL"), llm_credential_configured=bool(os.getenv("LLM_API_KEY")),
        )
        report["raw_contract"] = spec.model_dump(mode="json")
        report["curated_contracts"] = [table.model_dump() for table in curated]
        report["steps"].append({"step": phase, "actor": "Python deterministic parser", "status": "PASS",
                                "detail": "3 raw + 4 curated managed table schemas extracted from BRD."})
        phase = "LLM understanding and action planning"
        plan, response = propose_execution_plan(
            document, spec, curated, get_provider(spec.llm_provider, model=spec.llm_model or ""),
        )
        report["llm"] = {"response": response, "plan": plan.model_dump()}
        if plan.unsupported_requirements or plan.unresolved_decisions:
            for item in [*plan.unsupported_requirements, *plan.unresolved_decisions]:
                report["issues"].append({"check": "LLM contract checklist", "result": item,
                                         "action": "Clarify the BRD or implement the missing capability, then rerun."})
            raise ContractBlocked("LLM plan identifies unsupported requirements or unresolved decisions; see plan.")
        report["steps"].append({"step": phase, "actor": "LLM + Python evidence/schema validation",
                                "status": "PASS", "detail": plan.summary})
        phase = "source validation"
        if any(path.suffix.lower() != ".csv" for path in sources):
            raise ContractBlocked("This BRD explicitly accepts CSV only; workbook inputs are not authorized.")
        prepared = inspect_sources(
            sources, spec.feeds, output_root / "staging_csv", run_id, now.date(), now.isoformat(), spec.gcs_bucket,
        )
        report["source_inventory"] = [source.item.to_dict() for source in prepared]
        if len(prepared) != 3 or {item.feed_id for item in prepared} != {feed.feed_id for feed in spec.feeds}:
            raise ContractBlocked("Each registered feed requires exactly one unambiguous source file in this batch.")
        for source in prepared:
            feed = next(feed for feed in spec.feeds if feed.feed_id == source.feed_id)
            with source.source_path.open(encoding="utf-8-sig", newline="") as handle:
                headers = next(csv.reader(handle))
            if headers != [column.name for column in feed.source_columns]:
                raise ContractBlocked("Source headers must match BRD case and documented order exactly.")
            object_path(source.feed_id, source.source_path.name, now.date())
            if not source.item.load_eligible:
                raise ContractBlocked(f"{source.source_path.name}: file validation failed; see source inventory.")
            if source.item.rejected_rows:
                raise ContractBlocked(f"{source.source_path.name}: rejected rows block this complete-input batch.")
        report["steps"].append({"step": phase, "actor": "Python validation", "status": "PASS",
                                "detail": f"{sum(item.item.accepted_rows for item in prepared)} accepted rows."})
        phase = "SQL compilation"
        sql = compile_transform_sql(spec, curated, resources["01 dataset"])
        sql_path = write_sql(sql, output_root / "sql" / "01", f"01_rainfall_transform_{run_id}.sql")
        report["sql_files"] = [str(sql_path)]
        report["steps"].append({"step": phase, "actor": "Python/Jinja compiler", "status": "PASS"})
        project = resources["Google Cloud project ID"]
        bq = bigquery.Client(project=project, location="US")
        gcs = storage.Client(project=project)
        phase = "cloud target preflight"
        wanted_tables = []
        raw_ids = {}
        for feed in spec.feeds:
            table_id = f"{project}.{spec.dataset_00}.{feed.table_00}"
            table = bigquery.Table(table_id, schema=[field_schema(column) for column in [
                *feed.source_columns, *spec.metadata_columns,
            ]])
            table.time_partitioning = bigquery.TimePartitioning(field=feed.partition_field)
            table.clustering_fields = feed.cluster_fields
            table.labels = LABELS
            wanted_tables.append((table, True))
            raw_ids[feed.feed_id] = table_id
        for contract in curated:
            table = bigquery.Table(contract.table_id, schema=[field_schema(column) for column in contract.columns])
            name = contract.table_id.rsplit(".", 1)[-1]
            _, _, _, partition, cluster = OPERATIONS[name]
            if partition:
                table.time_partitioning = bigquery.TimePartitioning(field=partition)
            table.clustering_fields = cluster
            table.labels = LABELS
            wanted_tables.append((table, False))
        for table, _ in wanted_tables:
            table_id = f"{table.project}.{table.dataset_id}.{table.table_id}"
            fields = [{
                "name": field.name,
                "type": f"ARRAY<{field.field_type}>" if field.mode == "REPEATED" else field.field_type,
                "nullable": field.mode == "NULLABLE",
                "description": field.description,
            } for field in table.schema]
            create_sql = SqlRenderer().render("managed_table.sql.j2", {
                "brd_sha256": spec.brd_sha256, "table_id": table_id, "columns": fields,
                "partition_by": table.time_partitioning.field if table.time_partitioning else None,
                "cluster_by": table.clustering_fields or [],
            })
            report["sql_files"].append(str(write_sql(
                create_sql, output_root / "sql" / table.table_id[:2], table.table_id + ".sql",
            )))
        actions = []
        for table, additions_allowed in wanted_tables:
            try:
                existing = bq.get_table(table)
            except NotFound:
                actions.append((table, None))
            else:
                additions = schema_actions(existing, table, allow_nullable_additions=additions_allowed)
                actions.append((table, additions))
        for dataset_id in (resources["00 dataset"], resources["01 dataset"]):
            try:
                existing_dataset = bq.get_dataset(f"{project}.{dataset_id}")
            except NotFound:
                continue
            if existing_dataset.location.upper() != "US":
                raise ContractBlocked("Existing dataset location conflicts with BRD US multi-region.")
        try:
            bucket = gcs.get_bucket(spec.gcs_bucket)
        except NotFound:
            bucket = None
        if bucket is not None and (
            bucket.location.upper() != "US"
            or not bucket.iam_configuration.uniform_bucket_level_access_enabled
            or bucket.iam_configuration.public_access_prevention != "enforced"
        ):
            raise ContractBlocked("Existing bucket does not satisfy BRD location/access protections.")
        report["schema_actions"] = [
            {"table": table.full_table_id, "action": "CREATE" if additions is None else (
                "ADD_NULLABLE_COLUMNS" if additions else "REUSE"
            ), "additions": [field.name for field in additions or []]}
            for table, additions in actions
        ]
        report["steps"].append({"step": phase, "actor": "Python/GCP read-only APIs", "status": "PASS"})
        if dry_run:
            report["status"] = "DRY_RUN_READY"
            return report
        phase = "BRD analytical acceptance examples"
        report["analytical_acceptance"] = verify_analytical_examples(bq, sql, project, spec.dataset_00)
        report["steps"].append({"step": phase, "actor": "Python/BigQuery temporary fixtures", "status": "PASS",
                                "detail": "60 mm WARNING, absent-day INCOMPLETE, 75% gate, exact 80% boundary."})
        phase = "bucket provisioning"
        if bucket is None:
            bucket = storage.Bucket(gcs, spec.gcs_bucket)
            bucket.iam_configuration.uniform_bucket_level_access_enabled = True
            bucket.iam_configuration.public_access_prevention = "enforced"
            bucket.labels = LABELS
            bucket = gcs.create_bucket(bucket, location="US")
            report["cloud_mutations"].append({"action": "CREATE_BUCKET", "bucket": bucket.name})
        report["steps"].append({"step": phase, "actor": "Python/GCS", "status": "PASS"})
        phase = "BRD upload"
        report["objects"].append(ensure_object(
            bucket, f"brd/{spec.brd_sha256}/{brd.name}", brd.read_bytes(),
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ))
        phase = "managed table provisioning"
        for dataset_id in (resources["00 dataset"], resources["01 dataset"]):
            dataset = bigquery.Dataset(f"{project}.{dataset_id}")
            dataset.location = "US"
            dataset.labels = LABELS
            bq.create_dataset(dataset, exists_ok=True)
        for table, additions in actions:
            if additions is None:
                bq.create_table(table)
                report["cloud_mutations"].append({"action": "CREATE_NATIVE_TABLE", "table": table.full_table_id})
            elif additions:
                for field in additions:
                    query = f"ALTER TABLE `{table.project}.{table.dataset_id}.{table.table_id}` ADD COLUMN IF NOT EXISTS `{field.name}` {field.field_type}"
                    bq.query(query, location="US").result()
                report["cloud_mutations"].append({"action": "ADD_NULLABLE_COLUMNS", "table": table.full_table_id})
        report["steps"].append({"step": phase, "actor": "Python/BigQuery", "status": "PASS",
                                "detail": "Seven managed tables created or schema-checked; no replacement."})
        phase = "source upload and managed 00 loading"
        for source in prepared:
            feed = next(feed for feed in spec.feeds if feed.feed_id == source.feed_id)
            path = object_path(source.feed_id, source.source_path.name, now.date())
            obj = ensure_object(bucket, path, source.source_path.read_bytes(), content_type="text/csv")
            report["objects"].append(obj)
            downloaded = bucket.blob(path, generation=int(obj["generation"])).download_as_bytes()
            if hashlib.sha256(downloaded).hexdigest() != obj["sha256"]:
                raise ContractBlocked("Downloaded GCS generation checksum does not match the source manifest.")
            records = csv.DictReader(io.StringIO(downloaded.decode("utf-8-sig")))
            rows = [typed_record(row, feed.source_columns) for row in records]
            report["jobs"].append(load_rows(
                bq, bucket, obj, raw_ids[feed.feed_id], feed.feed_id,
                [*feed.source_columns, *spec.metadata_columns], rows, now.date(), now, spec.brd_sha256,
                {"source_file_name": "filename", "ingested_at": "ingested_at", "run_date": "run_date"},
            ))
            write_report(report, directory)
        report["steps"].append({"step": phase, "actor": "Python/GCS/BigQuery load jobs", "status": "PASS",
                                "detail": "Metadata added outside source contract; atomic append jobs reused on retry."})
        phase = "analytical 01 dry-run and atomic publication"
        query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_date", "DATE", now.date()),
                bigquery.ScalarQueryParameter("transformed_at", "TIMESTAMP", now),
            ], maximum_bytes_billed=1024**3, labels=LABELS,
        )
        validation_config = bigquery.QueryJobConfig(
            query_parameters=query_config.query_parameters, dry_run=True, use_query_cache=False,
            maximum_bytes_billed=1024**3, labels=LABELS,
        )
        dry_job = bq.query(sql, job_config=validation_config, location="US")
        report["sql_dry_run"] = {"status": "PASS", "estimated_bytes": dry_job.total_bytes_processed,
                                  "note": "BigQuery script dry-run does not validate every temporary-table statement."}
        # A new attempt may retry a previously failed script; successful scripts are replay-safe MERGEs.
        job = bq.query(sql, job_config=query_config, location="US")
        counts = list(job.result())
        report["jobs"].append({"phase": "01", "job_id": job.job_id})
        report["merge_statistics"] = []
        for child in bq.list_jobs(parent_job=job.job_id):
            if child.statement_type == "MERGE":
                completed_child = bq.get_job(child.job_id, location="US")
                stats = completed_child.dml_stats
                report["merge_statistics"].append({
                    "job_id": child.job_id,
                    "inserted": stats.inserted_row_count,
                    "updated": stats.updated_row_count,
                    "deleted": stats.deleted_row_count,
                })
        report["row_counts"].update({row["table_name"]: row["row_count"] for row in counts})
        report["steps"].append({"step": phase, "actor": "Python/BigQuery verified template",
                                "status": "PASS", "detail": "All four outputs validated then MERGEd atomically."})
        phase = "post-execution reconciliation"
        for table, _ in wanted_tables:
            actual = bq.get_table(table)
            schema_actions(actual, table, allow_nullable_additions=False)
            report["row_counts"][table.table_id] = actual.num_rows
        dates = bq.query(
            f"SELECT MIN(observation_date) AS start_date, MAX(observation_date) AS end_date "
            f"FROM `{project}.{spec.dataset_00}.00_rain_gauge_daily_observations`",
            job_config=bigquery.QueryJobConfig(maximum_bytes_billed=1024**3, labels=LABELS),
        ).result()
        report["affected_observation_range"] = [dict(row) for row in dates]
        report["steps"].append({"step": phase, "actor": "Python/BigQuery read-only metadata",
                                "status": "PASS", "detail": "Seven native table schemas, layouts and row counts verified."})
        if verify_retry:
            phase = "unchanged-input idempotency verification"
            retry_parameters = [
                bigquery.ScalarQueryParameter("run_date", "DATE", now.date()),
                bigquery.ScalarQueryParameter("transformed_at", "TIMESTAMP", datetime.now(timezone.utc)),
            ]
            report["idempotency_verification"] = verify_unchanged_retry(bq, sql, retry_parameters, 4)
            report["steps"].append({"step": phase, "actor": "Python/BigQuery actual retry", "status": "PASS",
                                    "detail": "Four MERGEs: zero inserted, updated or deleted rows."})
        report["status"] = "SUCCESS"
        phase = "report publication"
        write_report(report, directory)
        attempt_prefix = f"run_reports/{run_id}/attempts/{now:%Y%m%dT%H%M%S%f}"
        for filename, content_type in (("run_report.json", "application/json"), ("run_report.md", "text/markdown")):
            ensure_object(bucket, f"{attempt_prefix}/{filename}", (directory / filename).read_bytes(), content_type=content_type)
        report["report_gcs_prefix"] = f"gs://{bucket.name}/{attempt_prefix}/"
        report["steps"].append({"step": phase, "actor": "Python/GCS", "status": "PASS"})
    except (ContractBlocked, ValueError, LLMError, GoogleAPICallError, GoogleAuthError, OSError) as error:
        report["status"] = "BLOCKED" if isinstance(error, ContractBlocked) else "FAILED"
        if isinstance(error, (ContractBlocked, ValueError)) and not isinstance(error, LLMError):
            reason = str(error)
        else:
            reason = f"{type(error).__name__}: upstream details withheld to protect credentials."
            if isinstance(error, GoogleAPICallError):
                # Only our controlled SQL assertion strings, never arbitrary upstream content.
                for marker in (
                    "BLOCKED: observation", "BLOCKED: missing", "BLOCKED: multiple", "BLOCKED: snapshot",
                    "FAILED: incomplete", "FAILED: overlapping", "FAILED: ambiguous",
                    "FAILED: inconsistent", "FAILED: required", "FAILED: duplicate",
                ):
                    if marker in str(error):
                        reason += f" SQL validation: {marker}."
        report["issues"].append({"check": phase, "result": reason,
                                 "action": "Correct the indicated contract/data/IAM issue and rerun; no destructive fallback."})
        report["steps"].append({"step": phase, "actor": "Python safety gate", "status": report["status"],
                                "detail": reason})
    finally:
        write_report(report, directory)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brd", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=PROJECT_ROOT / "generated")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-idempotency", action="store_true")
    args = parser.parse_args()
    report = execute(args.brd, args.source_root, args.output_root, dry_run=args.dry_run,
                     verify_retry=args.verify_idempotency)
    print(json.dumps({key: report.get(key) for key in (
        "status", "run_id", "report_directory", "row_counts", "issues",
    )}, indent=2))
    return 0 if report["status"] in {"SUCCESS", "DRY_RUN_READY"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
