# BRD-to-BigQuery Agent

An internal, BRD-driven Python framework for interpreting business requirements,
validating source files, and building BigQuery `00` and `01` data layers. It
combines LLM-based requirement interpretation with deterministic Python
validation and controlled Google Cloud execution.

> **Important:** A BRD is an input specification, not permission to guess. If
> physical schemas, mappings, dependencies, validation rules, or rerun behavior
> are missing or contradictory, the run must stop and write a correction
> report. This framework does not promise to execute arbitrary prose.

## Contents

- [What happens during a run](#what-happens-during-a-run)
- [Repository structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Setup on a new machine](#setup-on-a-new-machine)
- [Place the input files](#place-the-input-files)
- [Configure the private environment file](#configure-the-private-environment-file)
- [Authenticate Google Cloud](#authenticate-google-cloud)
- [Run the framework](#run-the-framework)
- [Reports, SQL, and cloud output](#reports-sql-and-cloud-output)
- [Safety, retry behavior, and limitations](#safety-retry-behavior-and-limitations)
- [Testing and CI](#testing-and-ci)
- [Troubleshooting](#troubleshooting)

## What happens during a run

```text
External input folder
  ├── one BRD (DOCX, text PDF, TXT, or Markdown)
  └── registered source files (CSV; or Excel only when specified by the BRD)
           │
           ▼
1. Python checks ADC authentication and finds the BRD
2. LLM interprets the BRD and proposes a structured plan
3. Python validates schemas, evidence, identifiers, dependencies, and SQL
4. Python validates source headers, types, keys, and documented row rules
5. Python inspects existing GCS and BigQuery targets
6. Python uploads the BRD and sources and loads native managed 00 tables
7. Python builds complete 01 outputs, validates them, and MERGEs them atomically
8. Python reconciles counts/jobs and writes Markdown + JSON reports
```

### LLM responsibilities

The configured LLM is used for BRD reasoning: interpreting relationships among
requirements, proposing a table/dependency plan, identifying mappings and
conditional requirements, and listing unresolved items. The LLM produces
structured proposals only. It does not receive credentials and cannot make GCS
or BigQuery mutations.

### Python responsibilities

Python performs authentication checks, strict Pydantic validation, evidence
checks, source matching, data validation, schema comparison, SQL compilation,
uploads, BigQuery jobs, retries, reconciliation, and reporting. A proposal is
not executable until it passes these deterministic checks.

The rainfall BRD currently has a specialized, tested compiler for its effective
station dimension, daily rainfall fact, watershed coverage, and rolling
assessment. Other BRDs use a generic proposal path: physical names and schema
details must be explicit; 01 mappings must be safe, read-only BigQuery `SELECT`
statements. SQLGlot and BigQuery validate syntax, referenced dependencies, and
output columns/types before data is loaded. Anything outside the supported
checks is reported as unsupported rather than guessed.

## Repository structure

| Path | Purpose |
| --- | --- |
| `executer.py` | Single-command root launcher; uses the project virtual environment when present |
| `src/orchestrator/executer.py` | Loads the private env file, discovers one BRD in the external input folder, dispatches execution |
| `src/orchestrator/managed_runner.py` | Chooses the rainfall compiler or generic validated workflow |
| `src/orchestrator/managed_contract.py` | Strict generic BRD contract, evidence checks, read-only SQL AST validation |
| `src/rainfall/` | Rainfall-specific extraction, validation, SQL, acceptance checks, and cloud execution |
| `src/brd_parser/` | DOCX, text-PDF, TXT, and Markdown reading and legacy contract extraction |
| `src/llm/` | Provider interfaces, API integration, prompts, and response handling |
| `src/file_prep/` | Legacy CSV/Excel profiling, conversion, and upload helpers |
| `src/bigquery/` | Legacy BigQuery table, load, and validation helpers |
| `src/sql_generator/` | SQL template rendering and legacy DDL/load generation |
| `src/approval/`, `src/audit/` | Legacy approval and execution-report helpers |
| `templates/` | Managed table and rainfall transformation SQL templates |
| `configs/dev.yaml` | Non-secret legacy configuration template; no project or bucket is pinned |
| `.env.example` | Placeholder-only local environment template |
| `tests/` | Unit and integration tests; cloud calls are mocked except explicit user-run execution |
| `.github/workflows/tests.yml` | Pull-request and push regression test workflow |
| `generated/` | Local SQL, staging, and reports; ignored by Git |
| `input/`, `inputs/`, `data/` | Local input areas; ignored by Git |

## Prerequisites

- Git and Python **3.11 or 3.12** recommended.
- Google Cloud SDK (`gcloud`) for Application Default Credentials.
- Access to the Google Cloud project, BigQuery datasets, and GCS bucket named
  in the approved BRD.
- An approved LLM provider, model, API endpoint if applicable, and API key.
- Network/VPN access to the configured LLM endpoint if the endpoint is private.

Python 3.9 was used during initial development and produced end-of-life and
LibreSSL warnings. Use a current supported Python for a new clone.

### Google Cloud permissions

The authenticated principal needs permissions appropriate to the BRD and
operation, commonly:

- BigQuery job creation in the project.
- Dataset/table create and data edit in the approved datasets.
- GCS bucket/object create/read permissions on the approved prefixes.
- Permission to create a bucket only when the BRD explicitly authorizes that
  and the bucket name is available.

Authentication does not imply authorization. A `403` is an IAM/API policy
issue; logging in again does not grant missing permissions.

## Setup on a new machine

```bash
git clone <internal-repository-url>
cd brd-to-bigquery-agent
python3.11 -m venv .venv
```

Activate the environment if desired:

```bash
# macOS/Linux
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The root launcher also finds `.venv` automatically, so after installation it
can be run without activating the environment.

## Place the input files

Keep project inputs **outside the Git checkout**. Create a folder with exactly
one BRD and its corresponding source files, for example:

```text
~/brd-runs/rainfall/
├── requirements.docx
├── daily_observations.csv
├── station_map.csv
└── thresholds.csv
```

Supported BRD extensions: `.docx`, text-based `.pdf`, `.txt`, `.md`, `.markdown`.
Image-only/scanned PDFs are not automatically OCRed. The input folder must
contain exactly one BRD; Office lock files beginning with `~$` are ignored.

Source files must match what the BRD documents. CSV is the default. Excel is
accepted only when the BRD specifies the workbook and sheet. Do not place
multiple projects' files in one input folder. The launcher does not copy input
files into the repository.

## Configure the private environment file

Copy the placeholder file to the ignored, local `.env`:

```bash
cp .env.example .env
chmod 600 .env
```

Edit `.env` with the external input directory and approved settings:

```dotenv
BRD_INPUT_DIR="/absolute/path/to/brd-runs/rainfall"
LLM_PROVIDER="litellm"
LLM_MODEL="<approved-provider-qualified-model>"
LLM_API_KEY="<paste-locally-never-commit>"
LLM_API_BASE="<approved-endpoint>"
GOOGLE_CLOUD_PROJECT="<approved-project-id-from-the-BRD>"
GOOGLE_CLOUD_LOCATION="US"
```

If the BRD contains a bucket/datasets, those are its contract values. The
following variables are for legacy commands and must match the approved
environment if those commands are used:

```dotenv
GCS_LANDING_BUCKET="<approved-bucket>"
BQ_DATASET_00="<approved-00-dataset>"
BQ_DATASET_01="<approved-01-dataset>"
```

The launcher reads simple `KEY=VALUE` lines as data; it does not execute the
file as shell code. Values exported in the current shell override values in
`.env`. Never put credentials in the BRD, source files, SQL, screenshots,
prompts, reports, or committed documentation. `.env`, `.env.*`, local inputs,
generated outputs, virtual environments, credential JSON, and private keys are
excluded by `.gitignore`. Verify with `git status --short` before committing.

For an existing installation, the launcher also accepts an explicit private
file using `--env-file /path/to/private.env` or `BRD_AGENT_ENV_FILE`. It falls
back to the legacy user-local settings file only if `.env` is absent.

## Authenticate Google Cloud

The launcher checks Application Default Credentials before LLM or cloud work.
If ADC is missing or expired, authenticate in your terminal:

```bash
gcloud auth application-default login
```

If you need the `gcloud` CLI authenticated separately:

```bash
gcloud auth login
```

Then confirm the ADC project in the private environment matches the BRD:

```dotenv
GOOGLE_CLOUD_PROJECT="<the-approved-BRD-project>"
```

The framework does not store Google credentials or service-account keys.
Supply credentials through ADC or an explicitly approved workload identity.

## Run the framework

Run a dry run first:

```bash
python3 executer.py --dry-run
```

Expected successful status: `DRY_RUN_READY`. The dry run calls the LLM and
performs read-only Google Cloud checks, but it does not create resources,
upload objects, or load data. Inspect the run report before continuing.

Execute the workflow:

```bash
python3 executer.py
```

Optionally replay the 01 transformation and require zero inserted, updated, or
deleted rows when inputs are unchanged:

```bash
python3 executer.py --verify-idempotency
```

Other options:

```bash
python3 executer.py --help
python3 executer.py --env-file /absolute/path/to/private.env --dry-run
```

The default command performs real GCS/BigQuery writes to the destinations
specified and authorized by the BRD and local project configuration. It does
not prompt for per-action confirmation. Review the BRD destinations and
permissions before running it.

The same folder can be used for a corrected BRD. Preserve exactly one BRD,
replace/update its associated source files, update `BRD_INPUT_DIR` if needed,
and rerun. A changed contract is revalidated; incompatible or destructive
changes stop with a correction report.

## Reports, SQL, and cloud output

Local generated files are written under:

```text
generated/
├── sql/00/             # 00 table DDL
├── sql/01/             # 01 DDL and transformation SQL
├── reports/            # per-run JSON and Markdown reports
└── staging_csv/        # temporary accepted-only staging data
```

These artifacts are ignored by Git because reports and staging files can
contain project details. Successful reports are also written to the BRD's
configured GCS `run_reports/` prefix. Reports include the BRD checksum, run
identity, LLM/automatic step breakdown, schema decisions, source counts,
cloud job IDs, validation results, and actionable blocking issues. Credentials
and full rejected source rows are not meant to be included.

## Safety, retry behavior, and limitations

- Only explicitly documented BRD contracts are eligible for automatic work.
- The LLM cannot execute SQL or cloud operations.
- Existing tables are inspected before mutation. Table replacement, type
  narrowing, nullability changes, and undocumented fields are blocked.
- GCS objects use create-only generation preconditions; different content at
  an occupied path is not overwritten.
- Raw BigQuery loads use stable job IDs and receipts to avoid duplicate loads.
- Curated outputs are validated before transactional `MERGE`.
- A failed run may leave successfully created resources or loaded raw data; the
  workflow does not claim a global rollback across GCS and BigQuery.
- The current executor is a **CLI batch workflow**. It is not a deployed
  object-finalized trigger or scheduler.
- For rainfall, incomplete or rejected batches currently block publication,
  and deletion of keys removed from a complete reference snapshot is blocked
  pending reviewed reconciliation.
- Generic transformations must be a single safe SELECT over explicitly
  declared dependencies. Unsupported calculations or validation rules block.
- Production use requires a reviewed BRD, data-source licensing/retention
  approval, correct IAM, monitoring, and an operational deployment plan.

## Testing and CI

Run all unit/integration tests locally:

```bash
.venv/bin/python -m pytest -q
```

Tests use mocked cloud APIs; ordinary test runs do not create GCP resources.
Tests that exercise the expanded rainfall BRD and its three private source
files are skipped when those files are absent (they are deliberately not
committed). To run those locally, set `BRD_INPUT_DIR` to the external folder
containing the rainfall BRD and its three source files before running pytest.
The successful rainfall DEV run described in local development records is not
performed by GitHub Actions.

The GitHub Actions workflow runs on Python 3.11 and 3.12 for pushes and pull
requests. It installs dependencies and runs pytest without LLM or GCP secrets.

## Troubleshooting

| Symptom | Meaning/action |
| --- | --- |
| `BRD_INPUT_DIR` missing | Set it in ignored `.env` to an external folder |
| Found 0 BRDs | Add the BRD to the configured input folder |
| Found multiple BRDs | Keep one BRD per run folder; move old documents elsewhere |
| ADC unavailable/expired | Run `gcloud auth application-default login` |
| `403` / permission denied | Ask the project administrator to grant required IAM; do not keep re-authenticating |
| LLM provider unavailable | Verify provider/model/key/API base in local `.env`; never paste the key into chat/logs |
| Header/schema mismatch | Correct the BRD or source file; read the Markdown correction report |
| Unsupported/ambiguous BRD requirement | Clarify the BRD; the framework intentionally refuses to guess |
| Existing object has different data | Choose a new BRD-approved object path or explicitly review the conflict; the launcher does not overwrite it |
| VPN/proxy error | Connect to the approved network and rerun |

## Internal use

This repository is intended for authorized internal use. Do not publish it
publicly or include licensed/customer/operational data. Add organization-
approved ownership, license, and contribution terms before making it available
outside the organization.
