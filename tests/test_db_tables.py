import pytest
from sqlalchemy import text

from p_plp.db import (
    CdmTable,
    read_sql_df,
    get_engine,
    list_cdm_tables,
    list_work_tables,
    read_table,
    execute_sql,
)


pytest.importorskip("duckdb_engine")


def _build_test_engine():
    engine = get_engine(source_name="eunomia", database_path=":memory:", cdm_schema="main", work_schema="plp_work")
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists main"))
        conn.execute(text("create table main.person (person_id integer)"))
        conn.execute(text("insert into main.person values (1), (2)"))
    return engine


def test_execute_sql_and_read_sql_df_roundtrip():
    engine = _build_test_engine()

    execute_sql(engine, "create table plp_work.example_table (id integer)")
    execute_sql(engine, "insert into plp_work.example_table values (:id)", {"id": 7})

    df = read_sql_df(engine, "select * from plp_work.example_table")

    assert len(df) == 1
    assert int(df.loc[0, "id"]) == 7


def test_read_table_accepts_string_and_enum():
    engine = _build_test_engine()
    schema = engine._plp_source_config.cdm_schema

    df_from_str = read_table(engine, "person", schema=schema, limit=1)
    df_from_enum = read_table(engine, CdmTable.PERSON, schema=schema, limit=1)

    assert list(df_from_str.columns) == ["person_id"]
    assert list(df_from_enum.columns) == ["person_id"]


def test_list_cdm_and_work_tables():
    engine = _build_test_engine()

    cdm_tables = list_cdm_tables(engine)
    work_tables = list_work_tables(engine)

    assert "person" in set(cdm_tables["table_name"])
    assert "target_cohort" in set(work_tables["table_name"])
    assert "outcome_cohort" in set(work_tables["table_name"])
