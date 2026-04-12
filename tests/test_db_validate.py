import pytest
from sqlalchemy import text

from p_plp.db import (
    get_engine,
    validate_connection,
    validate_schemas,
    validate_tables,
)


pytest.importorskip("duckdb_engine")


def _build_test_engine(with_work_schema: bool = True):
    engine = get_engine(
        source_name="eunomia",
        database_path=":memory:",
        cdm_schema="main",
        vocabulary_schema="main",
        work_schema="plp_work" if with_work_schema else None,
    )
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists main"))
        conn.execute(text("create table main.person (person_id integer)"))
        conn.execute(text("create table main.observation_period (person_id integer)"))
        conn.execute(text("create table main.cohort (cohort_definition_id integer)"))
        if with_work_schema:
            conn.execute(text("create schema if not exists plp_work"))
            conn.execute(text("create table plp_work.target_cohort (subject_id integer)"))
    return engine


def test_validate_connection():
    engine = _build_test_engine()
    assert validate_connection(engine) is True


def test_validate_schemas_with_existing_cdm_and_work_schema():
    engine = _build_test_engine()
    assert validate_schemas(engine, require_work_schema=True) is True


def test_validate_schemas_requires_configured_work_schema():
    engine = _build_test_engine(with_work_schema=False)
    with pytest.raises(ValueError, match="work schema"):
        validate_schemas(engine, require_work_schema=True)


def test_validate_tables_accepts_existing_required_tables():
    engine = _build_test_engine()
    assert validate_tables(
        engine,
        required_cdm_tables=["person", "observation_period", "cohort"],
        required_work_tables=["target_cohort"],
    ) is True


def test_validate_tables_rejects_missing_cdm_table():
    engine = _build_test_engine()
    with pytest.raises(ValueError, match="Missing required CDM tables"):
        validate_tables(engine, required_cdm_tables=["condition_occurrence"])


def test_validate_tables_rejects_missing_work_table():
    engine = _build_test_engine()
    with pytest.raises(ValueError, match="Missing required work tables"):
        validate_tables(engine, required_work_tables=["outcome_cohort"])


def test_get_engine_can_validate_schemas_and_tables():
    engine = get_engine(
        source_name="eunomia",
        database_path=":memory:",
        cdm_schema="main",
        vocabulary_schema="main",
        work_schema="plp_work",
    )
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists main"))
        conn.execute(text("create schema if not exists plp_work"))
        conn.execute(text("create table main.person (person_id integer)"))
        conn.execute(text("create table main.cohort (cohort_definition_id integer)"))
        conn.execute(text("create table plp_work.target_cohort (subject_id integer)"))

    assert validate_schemas(engine, require_work_schema=True) is True
    assert validate_tables(
        engine,
        required_cdm_tables=["person", "cohort"],
        required_work_tables=["target_cohort"],
    ) is True
