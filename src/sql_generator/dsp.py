"""Explicit-column CLI adaptation of the DSP staging, DQ, refresh and audit pattern."""

from __future__ import annotations

from sqlglot import exp, parse_one

from ..orchestrator.managed_contract import ManagedTable
from .render_template import SqlRenderer

DSP_VERSION = "dsp_cli_02_v1 (reference DSP 3.2.0)"


def stage_name(table: ManagedTable) -> str:
    return "build_" + table.table_id.rsplit(".", 1)[-1]


def compile_dsp_publication(
    tables: list[ManagedTable], *, initialization: str = "", validation_sql: str = "",
    scopes: dict[str, str] | None = None, audit_sql: str = "",
) -> str:
    """Only deterministic code emits mutations; LLM SELECTs cannot supply script steps."""
    builds, checks, merges = [], [], []
    available: dict[str, str] = {}
    for table in tables:
        if table.layer != "02":
            raise ValueError("DSP KPI publication accepts only 02 contracts.")
        source = stage_name(table)
        tree = parse_one(table.select_sql, read="bigquery")
        for reference in tree.find_all(exp.Table):
            identifier = ".".join((reference.catalog, reference.db, reference.name))
            if identifier in available:
                reference.set("this", exp.to_identifier(available[identifier]))
                reference.set("db", None)
                reference.set("catalog", None)
        builds.append(f"CREATE TEMP TABLE {source} AS {tree.sql(dialect='bigquery')};")
        keys = ", ".join(f"`{key}`" for key in table.business_keys)
        required = " OR ".join(f"`{column.name}` IS NULL" for column in table.columns if not column.nullable)
        key_json = "TO_JSON_STRING(STRUCT(" + keys + "))"
        def violation(rule: str, predicate: str, observed: str = "'NULL required field or invalid array element'") -> str:
            return (
                f"SELECT '{table.table_id}' table_name, {key_json} business_key, "
                f"'{rule}' rule, {observed} observed_value FROM {source} s WHERE {predicate}"
            )
        checks.append(
            f"SELECT '{table.table_id}' table_name, {key_json} business_key, "
            f"'unique_key' rule, CAST(COUNT(*) AS STRING) observed_value "
            f"FROM {source} GROUP BY {keys} HAVING COUNT(*) > 1"
        )
        if required:
            checks.append(violation("required_fields", required))
        for column in table.columns:
            if column.type.startswith("ARRAY<"):
                checks.append(violation(
                    "array_elements_" + column.name,
                    f"EXISTS (SELECT 1 FROM UNNEST(`{column.name}`) element WHERE element IS NULL)",
                ))
        names = [column.name for column in table.columns]
        comparable = [name for name in names if name not in {"transformed_at", "transform_run_date"}]
        left = ", ".join(f"t.`{name}`" for name in comparable)
        right = ", ".join(f"s.`{name}`" for name in comparable)
        on = " AND ".join(f"t.`{key}` = s.`{key}`" for key in table.business_keys)
        runtime_fields = [name for name in names if name in {"transformed_at", "transform_run_date"}]
        if runtime_fields:
            assignments = ", ".join(f"`{name}` = t.`{name}`" for name in runtime_fields)
            builds.append(
                f"UPDATE {source} s SET {assignments} FROM `{table.table_id}` t "
                f"WHERE {on} AND TO_JSON_STRING(STRUCT({left})) = TO_JSON_STRING(STRUCT({right}));"
            )
        updates = ", ".join(f"`{name}` = s.`{name}`" for name in names if name not in table.business_keys)
        scope = (scopes or {}).get(table.table_id)
        using = f"(SELECT * FROM {source} WHERE {scope.replace('t.', '')})" if scope else source
        merge = f"MERGE `{table.table_id}` t USING {using} s ON {on}\n"
        if updates and comparable:
            merge += (
                f"WHEN MATCHED AND TO_JSON_STRING(STRUCT({left})) != TO_JSON_STRING(STRUCT({right})) "
                f"THEN UPDATE SET {updates}\n"
            )
        merge += (
            f"WHEN NOT MATCHED THEN INSERT ({', '.join('`' + name + '`' for name in names)}) "
            f"VALUES ({', '.join('s.`' + name + '`' for name in names)})\n"
        )
        if scope:
            merge += f"WHEN NOT MATCHED BY SOURCE AND {scope} THEN DELETE\n"
        merges.append(merge + ";")
        if scope and any(table.table_id in dependent.dependencies for dependent in tables):
            effective = "published_" + source
            builds.append(
                f"CREATE TEMP TABLE {effective} AS "
                f"SELECT * FROM {source} WHERE {scope.replace('t.', '')} "
                f"UNION ALL SELECT t.* FROM `{table.table_id}` t WHERE NOT ({scope}) "
                f"AND DATE_TRUNC(t.`{table.partition_by}`, MONTH) IN UNNEST(@affected_months);"
            )
            available[table.table_id] = effective
        else:
            available[table.table_id] = source
    return SqlRenderer().render("02_dsp_cli.sql.j2", {
        "template_version": DSP_VERSION, "initialization": initialization,
        "build_sql": "\n\n".join(builds), "checks_sql": "\nUNION ALL\n".join(checks),
        "validation_sql": validation_sql, "merge_sql": "\n\n".join(merges),
        "audit_sql": audit_sql,
    })
