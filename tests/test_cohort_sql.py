import pytest
from sqlalchemy import text

from p_plp.cohorts import execute_atlas_sql, load_cohort_from_sql, read_cohort_table
from p_plp.config import PipelineRunConfig, PredictionProblemConfig
from p_plp.db import get_engine
from p_plp.pipeline import build_pipeline_plan


pytest.importorskip("duckdb_engine")


def _build_test_engine():
    engine = get_engine(source_name="eunomia", database_path=":memory:", cdm_schema="main", work_schema="plp_work")
    with engine.begin() as conn:
        conn.execute(text("create schema if not exists main"))
        conn.execute(text("create table main.person (person_id integer)"))
        conn.execute(text("insert into main.person values (1), (2)"))
    return engine


def test_load_cohort_from_sql_reads_target_cohort():
    engine = _build_test_engine()

    df = load_cohort_from_sql(
        engine,
        table_name="target_cohort",
        sql="""
        drop table if exists plp_work.target_cohort;
        create table plp_work.target_cohort as
        select
            person_id as subject_id,
            DATE '2020-01-01' as cohort_start_date,
            DATE '2020-01-02' as cohort_end_date
        from main.person
        where person_id = 1;
        """,
    )

    assert len(df) == 1
    assert int(df.loc[0, "subject_id"]) == 1


def test_execute_atlas_sql_and_read_cohort_table():
    engine = _build_test_engine()

    execute_atlas_sql(
        engine,
        sql="""
        drop table if exists plp_work.target_cohort;
        create table plp_work.target_cohort as
        select
            person_id as subject_id,
            DATE '2020-01-01' as cohort_start_date,
            DATE '2020-01-29' as cohort_end_date
        from main.person
        order by person_id;
        """,
    )

    df = read_cohort_table(engine, table_name="target_cohort", limit=1)

    assert len(df) == 1
    assert int(df.loc[0, "subject_id"]) == 1


def test_load_cohort_from_sql_reads_outcome_cohort_with_limit():
    engine = _build_test_engine()

    df = load_cohort_from_sql(
        engine,
        table_name="outcome_cohort",
        sql="""
        drop table if exists plp_work.outcome_cohort;
        create table plp_work.outcome_cohort as
        select
            person_id as subject_id,
            DATE '2020-02-01' as cohort_start_date,
            DATE '2020-02-03' as cohort_end_date
        from main.person
        order by person_id;
        """,
        limit=1,
    )

    assert len(df) == 1
    assert int(df.loc[0, "subject_id"]) == 1


def test_execute_atlas_sql_requires_non_empty_sql():
    engine = _build_test_engine()

    with pytest.raises(ValueError, match="Atlas SQL"):
        execute_atlas_sql(engine, sql="   ")


def test_build_pipeline_plan_requires_target_and_outcome_sql():
    with pytest.raises(ValueError, match="target_cohort_sql"):
        build_pipeline_plan(
            PipelineRunConfig(
                problem=PredictionProblemConfig(outcome_cohort_sql="select 1"),
            )
        )

    with pytest.raises(ValueError, match="outcome_cohort_sql"):
        build_pipeline_plan(
            PipelineRunConfig(
                problem=PredictionProblemConfig(target_cohort_sql="select 1"),
            )
        )
