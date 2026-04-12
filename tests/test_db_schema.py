import pytest
from sqlalchemy import text

from p_plp.db import get_engine
from p_plp.db.schema import ensure_schema, reset_schema


pytest.importorskip("duckdb_engine")


def test_ensure_schema_creates_expected_work_tables():
    engine = get_engine(source_name="eunomia", database_path=":memory:", work_schema="plp_work")

    ensure_schema(engine, "plp_work")

    with engine.connect() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                text(
                    """
                    select table_name
                    from information_schema.tables
                    where table_schema = 'plp_work'
                    """
                )
            )
        }

    assert "target_cohort" in tables
    assert "outcome_cohort" in tables


def test_reset_schema_recreates_empty_schema():
    engine = get_engine(source_name="eunomia", database_path=":memory:", work_schema="plp_work")

    with engine.begin() as conn:
        conn.execute(text("create table plp_work.temp_table (id integer)"))

    reset_schema(engine, "plp_work")

    with engine.connect() as conn:
        tables = {
            row[0]
            for row in conn.execute(
                text(
                    """
                    select table_name
                    from information_schema.tables
                    where table_schema = 'plp_work'
                    """
                )
            )
        }

    assert "temp_table" not in tables
