import pytest
from sqlalchemy import text

from p_plp.db import get_engine, read_table


pytest.importorskip("duckdb_engine")


def test_read_table_basic():
    engine = get_engine(source_name="eunomia", database_path=":memory:", cdm_schema="main")
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists main"))
        conn.execute(text("create table main.person (person_id integer)"))
        conn.execute(text("insert into main.person values (1), (2)"))

    df = read_table(engine, "person", schema="main", limit=1)
    assert len(df) == 1
    assert list(df.columns) == ["person_id"]
