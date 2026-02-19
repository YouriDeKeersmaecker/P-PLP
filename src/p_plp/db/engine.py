from sqlalchemy import create_engine, text
from .config import DATABASE_URL, WORK_SCHEMA

RESET_SCHEMA=True


def get_engine():
    engine = create_engine(DATABASE_URL, future=True)

    if RESET_SCHEMA:
        _reset_schema(engine, WORK_SCHEMA)

    _ensure_schema(engine, WORK_SCHEMA)

    return engine


def _reset_schema(engine, schema):
    ddl = f"""
    DROP SCHEMA IF EXISTS {schema} CASCADE;
    CREATE SCHEMA {schema};
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _ensure_schema(engine, schema):
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {schema}.target_cohort (
        subject_id BIGINT,
        cohort_start_date DATE,
        cohort_end_date DATE
    );

    CREATE TABLE IF NOT EXISTS {schema}.outcome_cohort (
        subject_id BIGINT,
        cohort_start_date DATE,
        cohort_end_date DATE
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
