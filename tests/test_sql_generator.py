import re

from src.sql_generator.generate_00_sql import generate_00_sql
from src.sql_generator.generate_01_sql import generate_01_sql
from src.sql_generator.generate_load_sql import generate_load_sql, is_destructive


def test_00_sql_matches_the_core_template(spec, brushfire_table, config):
    sql = generate_00_sql(spec, brushfire_table, config)
    assert "CREATE OR REPLACE EXTERNAL TABLE" in sql
    assert "`clgx-surveillance-dev-9e5e.ampd_00_hazard_wildfire.00_wfrs_brushfire_changelogs`" in sql
    assert 'format="csv"' in sql
    assert "skip_leading_rows=1" in sql
    assert 'quote=""' in sql
    assert "allow_jagged_rows=false" in sql


def _column_block(sql: str) -> str:
    ddl = sql[sql.index("CREATE"):]
    return ddl[ddl.index("(") + 1: ddl.index(")")]


def test_00_sql_declares_every_column_as_string(spec, brushfire_table, config):
    sql = generate_00_sql(spec, brushfire_table, config)
    body = _column_block(sql)
    declared = re.findall(r"^\s*(\w+)\s+(\w+),?$", body, re.MULTILINE)
    assert declared, body
    assert {t for _, t in declared} == {"STRING"}
    # LOAD_DATE_01 is stamped during the load, so it is not part of the 00 table.
    assert "LOAD_DATE_01" not in body
    assert "run_date" in body and "Source_FileName" in body


def test_00_sql_uses_a_wildcard_uri(spec, brushfire_table, config):
    sql = generate_00_sql(spec, brushfire_table, config)
    expected = "gs://clgx-surveillance-dev-hazard/00_wfrs_brushfire_changelogs/wfrs_brushfire_changelogs_*.csv"
    assert f"uris=['{expected}']" in sql


def test_01_sql_partitions_and_clusters(spec, brushfire_table, config):
    sql = generate_01_sql(spec, brushfire_table, config)
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "`clgx-surveillance-dev-9e5e.ampd_01_hazard_wildfire.01_wfrs_brushfire_changelogs_county`" in sql
    assert "PARTITION BY run_date" in sql
    assert "CLUSTER BY countyFIPSCode" in sql
    assert "countyFIPSCode STRING" in sql
    assert "riskValue INTEGER" in sql
    assert "previousAcres FLOAT64" in sql
    assert "LOAD_DATE_01 DATE NOT NULL" in sql


def test_load_sql_deletes_then_inserts_the_run(spec, brushfire_table, config):
    sql = generate_load_sql(spec, brushfire_table, config)
    assert "BEGIN TRANSACTION;" in sql
    assert "COMMIT TRANSACTION;" in sql
    delete_at = sql.index("DELETE FROM")
    insert_at = sql.index("INSERT INTO")
    assert delete_at < insert_at, "the delete must run before the insert"
    assert "WHERE run_date = DATE '2026-04-01'" in sql
    assert is_destructive(sql)


def test_load_sql_casts_strings_to_target_types(spec, brushfire_table, config):
    sql = generate_load_sql(spec, brushfire_table, config)
    assert "SAFE_CAST(REGEXP_REPLACE(NULLIF(TRIM(src.riskValue), ''), r'[,$%\\s]', '') AS INT64) AS riskValue" in sql
    # countyFIPSCode is zero-padded: LPAD repairs sources that lost leading zeros.
    assert "LPAD(NULLIF(TRIM(src.countyFIPSCode), ''), 5, '0') AS countyFIPSCode" in sql
    assert "CURRENT_DATE() AS LOAD_DATE_01" in sql
    assert "DATE '2026-04-01' AS run_date" in sql


def test_load_sql_maps_profiled_source_headers(spec, brushfire_table, config):
    """A snake_case source with no run_date must still satisfy the BRD contract."""
    brushfire_table.source_headers = ["state", "county_fips_code", "risk_value"]
    brushfire_table.source_map = {"countyFIPSCode": "county_fips_code", "riskValue": "risk_value"}
    brushfire_table.source_missing = ["run_date", "Source_FileName"]

    sql = generate_load_sql(spec, brushfire_table, config)
    assert "LPAD(NULLIF(TRIM(src.county_fips_code), ''), 5, '0') AS countyFIPSCode" in sql
    assert "REGEXP_EXTRACT(src._FILE_NAME, r'[^/]+$') AS Source_FileName" in sql
    assert "DATE '2026-04-01' AS run_date" in sql
    # No run column in the file, so the load must not filter it away.
    assert "WHERE TRUE" in sql


def test_load_sql_filters_the_00_table_by_run_date(spec, brushfire_table, config):
    sql = generate_load_sql(spec, brushfire_table, config)
    assert "SAFE_CAST(NULLIF(TRIM(src.run_date), '') AS DATE) = DATE '2026-04-01'" in sql


def test_append_strategy_generates_no_delete(spec, brushfire_table, config):
    brushfire_table.rerun_strategy = "append"
    sql = generate_load_sql(spec, brushfire_table, config)
    assert "DELETE FROM" not in sql
    assert "INSERT INTO" in sql


def test_truncate_strategy_generates_truncate(spec, brushfire_table, config):
    brushfire_table.rerun_strategy = "truncate_table"
    sql = generate_load_sql(spec, brushfire_table, config)
    assert "TRUNCATE TABLE" in sql


def test_state_grain_table_clusters_by_state_code(spec, config):
    from src.models.pipeline_spec import ColumnSpec, TableSpec

    table = TableSpec(
        source_name="Drought",
        grain="state",
        table_00="00_wfrs_drought_changelogs",
        table_01="01_wfrs_drought_changelogs_state",
        gcs_folder="00_wfrs_drought_changelogs",
        file_pattern="wfrs_drought_changelogs_",
        schema=[
            ColumnSpec(name="stateCode", type="STRING", preserve_leading_zeros=True),
            ColumnSpec(name="exPoints", type="INTEGER"),
            ColumnSpec(name="netStatePctChg", type="FLOAT64"),
            ColumnSpec(name="run_date", type="DATE", is_metadata=True, nullable=False),
            ColumnSpec(name="Source_FileName", type="STRING", is_metadata=True),
            ColumnSpec(name="LOAD_DATE_01", type="DATE", is_metadata=True, added_at_load=True, nullable=False),
        ],
        cluster_by=["stateCode"],
    )
    sql = generate_01_sql(spec, table, config)
    assert "CLUSTER BY stateCode" in sql
    assert "stateCode STRING" in sql
