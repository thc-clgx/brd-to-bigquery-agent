"""Read-only readiness checks for the expanded managed-00/01 rainfall contract."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ..brd_parser.document_reader import BrdDocument, read_document
from ..llm.provider import get_provider
from ..utils.config import PROJECT_ROOT
from .llm_analysis import analyze_rainfall_brd
from .processing import (
    discover_source_files,
    extract_pipeline_contract,
    inspect_sources,
    make_run_identity,
    sha256_file,
)


class CuratedColumn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    nullable: bool
    definition: str

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError("Invalid curated column identifier.")
        return value

    @field_validator("type")
    @classmethod
    def valid_type(cls, value: str) -> str:
        if not re.fullmatch(
            r"(?:STRING|DATE|FLOAT64|INT64|TIMESTAMP|BOOL|ARRAY<(?:STRING|DATE|TIMESTAMP)>)",
            value,
        ):
            raise ValueError("Unsupported curated BigQuery type.")
        return value


class CuratedTable(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table_id: str
    columns: list[CuratedColumn]
    section: Literal["8.3"] = "8.3"

    @model_validator(mode="after")
    def valid_schema(self) -> "CuratedTable":
        if not re.fullmatch(
            r"[a-z][a-z0-9-]*\.[A-Za-z_][A-Za-z0-9_]*\.01_[A-Za-z0-9_]+",
            self.table_id,
        ):
            raise ValueError("Invalid fully qualified curated table identifier.")
        names = [column.name for column in self.columns]
        if not names or len(names) != len(set(names)):
            raise ValueError("A curated table requires distinct, nonempty columns.")
        return self


def development_resources(document: BrdDocument) -> dict[str, str]:
    expected = {
        "Environment",
        "Google Cloud project ID",
        "Runtime region",
        "GCS landing bucket",
        "00 dataset",
        "01 dataset",
    }
    optional = {"Pipeline service account", "02 dataset"}
    resources: dict[str, str] = {}
    for rows in document.tables:
        if not rows or rows[0][:2] != ["Resource", "Development value"]:
            continue
        for row in rows[1:]:
            if len(row) >= 2 and row[0] in expected | optional:
                if row[0] in resources:
                    raise ValueError("Duplicate DEV resource declaration.")
                resources[row[0]] = row[1]
    if not expected <= set(resources):
        raise ValueError("The expanded BRD must declare every DEV execution resource.")
    if resources["Environment"] != "dev":
        raise ValueError("Automatic execution is restricted to DEV.")
    project = resources["Google Cloud project ID"]
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", project):
        raise ValueError("Invalid BRD Google Cloud project identifier.")
    if resources["Runtime region"] != "us-central1":
        raise ValueError("Unsupported DEV runtime region.")
    for key in ("00 dataset", "01 dataset", *(["02 dataset"] if "02 dataset" in resources else [])):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", resources[key]):
            raise ValueError("Invalid BRD dataset identifier.")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]", resources["GCS landing bucket"]):
        raise ValueError("Invalid BRD bucket identifier.")
    target = resources.get("Pipeline service account")
    if target is not None and not re.fullmatch(
        rf"[a-z][a-z0-9-]*@{re.escape(project)}\.iam\.gserviceaccount\.com",
        target,
    ):
        raise ValueError("Invalid BRD workload identity.")
    return resources


def approved_bucket(resources: dict[str, str]) -> tuple[str, dict | None]:
    """Bind a reviewed DEV naming override to its original project and BRD bucket."""
    original = resources["GCS landing bucket"]
    override = os.getenv("GCS_LANDING_BUCKET_OVERRIDE")
    if not override or override == original:
        return original, None
    approval = f"APPROVE_DEV:{resources['Google Cloud project ID']}:{original}:{override}"
    if resources["Environment"] != "dev" or os.getenv("GCS_BUCKET_OVERRIDE_APPROVAL") != approval:
        raise ValueError("Bucket override requires a matching explicit DEV deployment approval.")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]", override):
        raise ValueError("Invalid approved bucket override.")
    return override, {
        "decision": "Approved DEV bucket-name override; business rules and BRD remain unchanged.",
        "brd_bucket": original, "deployment_bucket": override,
        "project": resources["Google Cloud project ID"],
    }


def extract_curated_contracts(document: BrdDocument) -> list[CuratedTable]:
    resources = development_resources(document)
    prefix = resources["Google Cloud project ID"] + "." + resources["01 dataset"] + "."
    identifiers = set(re.findall(
        r"\b[a-z][a-z0-9-]*\.[A-Za-z_][A-Za-z0-9_]*\.01_[A-Za-z0-9_]+\b",
        document.text,
    ))
    signatures = {
        "effective_start_date": "01_dim_station_watershed",
        "quality_flag": "01_fact_daily_station_rainfall",
        "rolling_rainfall_mm": "01_fact_station_rainfall_assessment",
        "expected_station_count": "01_fact_watershed_station_coverage",
    }
    expected = {prefix + name for name in signatures.values()}
    if identifiers != expected:
        raise ValueError("The physical 01 tables conflict with the DEV dataset or supported contract.")
    result: dict[str, CuratedTable] = {}
    for rows in document.tables:
        if not rows or rows[0][:3] != ["Column", "BigQuery type", "Nullable"]:
            continue
        columns = []
        for row in rows[1:]:
            if len(row) != 4 or row[2] not in {"Yes", "No"}:
                raise ValueError("Malformed curated field contract or nullability.")
            columns.append(CuratedColumn(
                name=row[0], type=row[1], nullable=row[2] == "Yes", definition=row[3],
            ))
        names = {column.name for column in columns}
        if any(name in names for name in (
            "minimum_coverage_source", "reporting_reliability_pct", "calendar_days_in_scope",
            "alert_proximity_rank",
        )):
            continue
        matches = [name for signature, name in signatures.items() if signature in names]
        if len(matches) != 1 or matches[0] in result:
            raise ValueError("Ambiguous or duplicated curated schema table.")
        table = matches[0]
        result[table] = CuratedTable(table_id=prefix + table, columns=columns)
    if set(result) != set(signatures.values()):
        raise ValueError("The BRD does not define all four curated schemas.")
    return list(result.values())


def verify_workload_identity(target: str | None = None) -> dict[str, str]:
    """Refresh ADC, or explicitly required impersonation, without displaying tokens."""
    import google.auth
    from google.auth import impersonated_credentials
    from google.auth.exceptions import DefaultCredentialsError, RefreshError, TransportError
    from google.auth.transport.requests import Request

    try:
        source, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        credentials = source
        if target is not None:
            credentials = impersonated_credentials.Credentials(
                source_credentials=source,
                target_principal=target,
                target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                lifetime=600,
            )
        credentials.refresh(Request())
    except DefaultCredentialsError:
        return {"status": "BLOCKED", "reason": "ADC_NOT_CONFIGURED"}
    except RefreshError as error:
        # Upstream bodies can contain credentials; retain only allowlisted classifications.
        text = str(error)
        reason = next(
            (code for code in ("NOT_FOUND", "PERMISSION_DENIED", "IAM_SERVICE_DISABLED") if code in text),
            "IMPERSONATION_REJECTED" if target else "ADC_REFRESH_REJECTED",
        )
        return {"status": "BLOCKED", "reason": reason}
    except TransportError:
        return {"status": "BLOCKED", "reason": "AUTH_TRANSPORT_ERROR"}
    if target is not None:
        return {"status": "PASS", "reason": "IMPERSONATION_VERIFIED", "target": target}
    return {"status": "PASS", "reason": "ADC_REFRESH_VERIFIED"}


def authentication_guidance(result: dict[str, str]) -> str:
    reason = result["reason"]
    if reason in {"PERMISSION_DENIED", "IAM_SERVICE_DISABLED", "NOT_FOUND", "IMPERSONATION_REJECTED"}:
        return "Google authentication/identity authorization is blocked; an administrator must verify the identity, IAM roles and enabled APIs. Re-login is not a permission fix."
    if reason == "AUTH_TRANSPORT_ERROR":
        return "Google authentication could not reach the endpoint; check VPN/network access and retry."
    return "ADC authentication is missing or expired. Run gcloud auth application-default login in your terminal."


def run_preflight(
    brd: Path, source_root: Path, output_root: Path, *, call_llm: bool, check_identity: bool,
) -> dict:
    document = read_document(brd)
    resources = development_resources(document)
    curated = extract_curated_contracts(document)
    today = datetime.now(timezone.utc).date()
    sources = discover_source_files(source_root)
    run_id, key, manifest = make_run_identity(sha256_file(brd), today, sources)
    directory = output_root / "reports" / f"strict_preflight_{run_id}"
    directory.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    spec, _ = extract_pipeline_contract(
        brd, run_date=today, run_id=run_id, idempotency_key=key, ingested_at_utc=now,
        project_id=resources["Google Cloud project ID"], location="US",
        gcs_bucket=resources["GCS landing bucket"], dataset_00=resources["00 dataset"],
        dataset_00_is_approved=True, llm_provider=os.getenv("LLM_PROVIDER"),
        llm_model=os.getenv("LLM_MODEL"), llm_credential_configured=bool(os.getenv("LLM_API_KEY")),
    )
    prepared = inspect_sources(
        sources, spec.feeds, output_root / "staging_csv", run_id, today, now,
        resources["GCS landing bucket"],
    )
    environment_differences = []
    for variable, resource in (
        ("GOOGLE_CLOUD_PROJECT", "Google Cloud project ID"),
        ("GCS_LANDING_BUCKET", "GCS landing bucket"),
        ("BQ_DATASET_00", "00 dataset"),
        ("BQ_DATASET_01", "01 dataset"),
    ):
        value = os.getenv(variable)
        if value and value != resources[resource]:
            environment_differences.append({
                "variable": variable, "brd_value": resources[resource],
                "resolution": "BRD value used for read-only planning; no environment override.",
            })
    blockers = []
    for source in prepared:
        if source.source_path.suffix != ".csv":
            blockers.append(f"{source.source_path.name}: section 5.6 accepts CSV only.")
        if not source.item.load_eligible:
            blockers.append(f"{source.source_path.name}: source validation failed.")
    if {source.feed_id for source in prepared} != {feed.feed_id for feed in spec.feeds}:
        blockers.append("Required source feeds are missing or ambiguous.")
    identity = verify_workload_identity(resources.get("Pipeline service account")) if check_identity else {
        "status": "NOT_CHECKED", "reason": "CLOUD_CHECK_NOT_REQUESTED",
    }
    if identity["status"] != "PASS":
        blockers.append("Google Cloud authentication is not verified; no cloud execution is permitted.")
    llm = {"status": "NOT_CALLED"}
    if call_llm:
        from ..llm.provider import LLMError
        from pydantic import ValidationError

        try:
            analysis, response = analyze_rainfall_brd(
                document, spec, [source.item for source in prepared],
                get_provider(spec.llm_provider, model=spec.llm_model or ""), max_attempts=2,
            )
        except (LLMError, ValidationError, ValueError) as error:
            llm = {"status": "FAILED", "error_type": type(error).__name__}
            blockers.append("Live LLM reasoning failed deterministic validation or transport.")
        else:
            llm = {"status": "PASS", "response": response.to_dict(), "analysis": analysis.model_dump()}
    else:
        blockers.append("Required live LLM reasoning has not run.")
    report = {
        "status": "BLOCKED" if blockers else "PREFLIGHT_READY",
        "mode": "STRICT_BRD_ONLY_READ_ONLY_PREFLIGHT",
        "run_id": run_id, "brd_file": brd.name, "brd_sha256": spec.brd_sha256,
        "resources": resources, "source_manifest": manifest,
        "environment_differences": environment_differences,
        "raw_contract": spec.model_dump(mode="json"),
        "curated_contracts": [table.model_dump() for table in curated],
        "source_inventory": [source.item.to_dict() for source in prepared],
        "workload_identity": identity, "llm": llm, "blockers": blockers,
        "cloud_mutations": [], "executed_sql": [], "report_directory": str(directory),
    }
    (directory / "run_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    lines = [
        "# Strict BRD-only readiness report", "", f"**Status: {report['status']}**",
        "", f"BRD: {brd.name}", f"SHA-256: {spec.brd_sha256}",
        "", "No cloud resources or data were created, uploaded, altered, or loaded.",
        "", "## Contract", "", "Extracted three managed 00 and four managed 01 schemas.",
        "", *[f"- {table.table_id}: {len(table.columns)} columns" for table in curated],
        "", "## Sources", "",
        *[f"- {source.item.source_file}: {source.item.accepted_rows} accepted, "
          f"{source.item.rejected_rows} rejected." for source in prepared],
        "", "## Live LLM", "", llm["status"],
        "", *(
            [llm["analysis"]["summary"], "", *[
                f"- {finding['statement']} (BRD: {finding['brd_section']})"
                for finding in llm["analysis"]["findings"]
            ]]
            if llm["status"] == "PASS" else []
        ),
        "", "## Environment alignment", "",
        *[
            f"- {item['variable']}: differs from the BRD; planning uses {item['brd_value']}."
            for item in environment_differences
        ],
        "", "## Blockers", "", *[f"- {blocker}" for blocker in blockers],
        "", "## Identity", "", json.dumps(identity),
    ]
    (directory / "run_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brd", required=True, type=Path)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=PROJECT_ROOT / "generated")
    parser.add_argument("--require-llm", action="store_true")
    parser.add_argument("--check-identity", action="store_true")
    args = parser.parse_args()
    report = run_preflight(
        args.brd, args.source_root, args.output_root,
        call_llm=args.require_llm, check_identity=args.check_identity,
    )
    print(json.dumps({
        "status": report["status"], "report_directory": report["report_directory"],
        "llm_status": report["llm"]["status"], "workload_identity": report["workload_identity"],
        "cloud_mutations": report["cloud_mutations"],
    }, indent=2))
    return 2 if report["status"] == "BLOCKED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
