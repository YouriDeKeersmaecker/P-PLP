import pytest
from sqlalchemy import text

from p_plp.db import get_engine


pytest.importorskip("duckdb_engine")


def test_connection():
    engine = get_engine(source_name="eunomia", database_path=":memory:", cdm_schema="main")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1


def test_connection_validation_can_run_during_engine_creation():
    engine = get_engine(
        source_name="eunomia",
        database_path=":memory:",
        cdm_schema="main",
    )
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        assert result.scalar() == 1
