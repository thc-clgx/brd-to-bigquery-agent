"""Prompts for the BRD extraction and decision agents.

Kept in one place so the domain knowledge can be reviewed and tuned without
touching control flow. Every prompt demands strict JSON: the response is parsed
and validated, never executed.
"""

from __future__ import annotations

import json
from typing import Any

SYSTEM_EXTRACTOR = """\
You are a senior data engineer who converts Business Requirements Documents \
(BRDs) into precise, machine-readable data-pipeline specifications.

You will be given the text of a BRD. Different teams write BRDs very \
differently: headings, bullets, tables, or flowing prose. Your job is to \
understand the intent and return a structured specification.

RULES
1. Return ONE JSON object and nothing else. No prose, no markdown fences.
2. Never invent table names, column names, datasets, buckets or projects. If \
the BRD does not state something, omit the field or use null.
3. Copy identifiers EXACTLY as written, including case and underscores \
(e.g. `00_wfrs_brushfire_changelogs_test`, `countyFIPSCode`).
4. Extract only the pipeline type you are asked about. A BRD often describes \
several pipelines; ignore the others.
5. Any column described as needing "leading zeros retained", or that is a \
FIPS/ZIP/state/county code, MUST be type STRING with \
preserve_leading_zeros=true. Storing it as a number is a data-loss bug.
6. Use BigQuery types: STRING, INT64, FLOAT64, NUMERIC, DATE, DATETIME, \
TIMESTAMP, BOOL. Map INTEGER->INT64 and FLOAT->FLOAT64.
7. Do not include audit columns that are added by the pipeline \
(run_date, Source_FileName, LOAD_DATE_01) in the column list. They are added \
automatically. Instead report them via the `directives` block.
8. If the BRD is ambiguous, pick the most reasonable reading and record it in \
`assumptions` with your confidence. Do not silently guess.
"""

SCHEMA_HINT: dict[str, Any] = {
    "project_id": "string | null",
    "gcs_bucket": "string | null",
    "dataset_00": "string | null",
    "dataset_01": "string | null",
    "schedule": "string | null",
    "historical_runs": ["YYYY_MM"],
    "directives": {
        "partition_by": "string | null",
        "cluster_candidates": ["string"],
        "load_date_field": "string | null",
        "rerun_strategy": "replace_run | append | truncate_table | null",
    },
    "tables": [
        {
            "source_name": "the BRD heading this came from",
            "table_00": "00_...",
            "table_01": "01_...",
            "gcs_folder": "string | null",
            "grain": "county | state | property | other",
            "source_location_hint": "gs://... path template from the BRD, or null",
            "source_format": "xlsx | csv | json | null",
            "partition_by": "explicit DATE partition field | null",
            "cluster_by": ["explicit clustering field"],
            "columns": [
                {
                    "name": "exact column name",
                    "type": "BigQuery type",
                    "nullable": "boolean: false when required, true when nullable",
                    "preserve_leading_zeros": "boolean",
                    "description": "string | null",
                }
            ],
        }
    ],
    "assumptions": [
        {"topic": "string", "assumption": "string", "confidence": "0.0-1.0"}
    ],
    "unsupported": ["anything the BRD asks for that this pipeline type cannot express"],
}


def extraction_prompt(document_text: str, pipeline_type: str, rule_hint: dict | None = None) -> str:
    """Ask the model for a full pipeline specification."""
    parts = [
        f"Extract the specification for the **{pipeline_type}** pipeline only.",
        "",
        "Return JSON shaped exactly like this (values are type hints, not examples):",
        json.dumps(SCHEMA_HINT, indent=2),
    ]
    if rule_hint:
        parts += [
            "",
            "A deterministic parser already extracted the following. Treat it as a "
            "strong hint: confirm it, correct it where the document disagrees, and "
            "fill in what it missed. Explain any correction in `assumptions`.",
            json.dumps(rule_hint, indent=2)[:6000],
        ]
    parts += ["", "--- BRD DOCUMENT START ---", document_text, "--- BRD DOCUMENT END ---"]
    return "\n".join(parts)


def repair_prompt(previous_json: str, failures: list[str], document_text: str) -> str:
    """Feed validation failures back so the model can self-correct."""
    return "\n".join([
        "Your previous specification FAILED validation. Fix it.",
        "",
        "Failures:",
        *(f"  - {failure}" for failure in failures),
        "",
        "Your previous answer:",
        previous_json[:8000],
        "",
        "Return the corrected JSON object only. Change only what is needed to "
        "resolve the failures; do not invent new tables or columns. If a failure "
        "cannot be fixed from the document, list it in `unsupported`.",
        "",
        "--- BRD DOCUMENT START ---",
        document_text,
        "--- BRD DOCUMENT END ---",
    ])


SYSTEM_DECIDER = """\
You are a data-engineering reviewer. You answer one narrow question about a \
data pipeline and report how confident you are.

Return ONE JSON object only:
{"answer": <the answer>, "confidence": 0.0-1.0, "rationale": "one sentence"}

Be honest about confidence. Below 0.7 means a human should check. Never guess \
a table, column, or dataset name that was not given to you.
"""


def decision_prompt(question: str, context: dict[str, Any], options: list[str] | None = None) -> str:
    parts = [question, "", "Context:", json.dumps(context, indent=2, default=str)[:4000]]
    if options:
        parts += ["", f"`answer` must be exactly one of: {json.dumps(options)}"]
    return "\n".join(parts)


SYSTEM_MAPPER = """\
You map physical file column headers onto the column names a specification \
requires. Headers drift: snake_case vs camelCase, abbreviations (chg/change, \
pct/percent), reordering, and extra columns.

Return ONE JSON object only:
{"mapping": {"<spec column>": "<physical header>"},
 "missing": ["spec columns with no physical counterpart"],
 "extra":   ["physical headers the spec does not describe"],
 "confidence": 0.0-1.0,
 "rationale": "one sentence"}

Map on meaning, not position. Never map two spec columns to the same header. \
If unsure, leave it in `missing` rather than guessing - a wrong mapping \
silently corrupts data, a missing one is caught by validation.
"""


def mapping_prompt(spec_columns: list[dict[str, Any]], headers: list[str], source_name: str) -> str:
    return "\n".join([
        f"Source: {source_name}",
        "",
        "Specification columns:",
        json.dumps(spec_columns, indent=2),
        "",
        "Physical headers found in the staged file, in order:",
        json.dumps(headers, indent=2),
    ])


__all__ = [
    "SYSTEM_EXTRACTOR", "SYSTEM_DECIDER", "SYSTEM_MAPPER", "SCHEMA_HINT",
    "extraction_prompt", "repair_prompt", "decision_prompt", "mapping_prompt",
]
