from p_plp.db import execute_sql, read_sql_df
from p_plp.db.config import get_engine_config


def execute_atlas_sql(
    engine,
    sql: str,
):
    """Execute raw Atlas cohort SQL against the connected database."""

    if not sql.strip():
        raise ValueError("Atlas SQL must not be empty.")

    execute_sql(engine, sql)


def read_atlas_cohort(
    engine,
    cohort_definition_id: int,
    limit: int | None = None,
):
    """Read generated cohort rows from the OMOP cohort table."""

    config = get_engine_config(engine)
    if config is None:
        raise ValueError("No engine configuration is attached.")

    cdm_schema = config.cdm_schema
    select_sql = f"""
    select *
    from {cdm_schema}.cohort
    where cohort_definition_id = :cohort_definition_id
    order by cohort_start_date, subject_id
    """
    params = {"cohort_definition_id": int(cohort_definition_id)}
    if limit is not None:
        select_sql += "\nlimit :limit"
        params["limit"] = int(limit)

    return read_sql_df(engine, select_sql, params)


def load_atlas_cohort(
    engine,
    sql: str,
    cohort_definition_id: int,
    limit: int | None = None,
):
    """Execute Atlas SQL and return the generated cohort rows as a DataFrame."""

    execute_atlas_sql(engine, sql)
    return read_atlas_cohort(
        engine,
        cohort_definition_id=cohort_definition_id,
        limit=limit,
    )


def load_atlas_cohort_to_work_table(
    engine,
    sql: str,
    cohort_definition_id: int,
    table_name: str,
    limit: int | None = None,
):
    """Execute Atlas SQL, then copy the cohort rows into a work table and return them."""

    config = get_engine_config(engine)
    if config is None:
        raise ValueError("No engine configuration is attached.")
    if not config.work_schema:
        raise ValueError("No work schema is configured.")

    load_atlas_cohort(
        engine,
        sql=sql,
        cohort_definition_id=cohort_definition_id,
        limit=None,
    )

    materialize_sql = f"""
    create schema if not exists {config.work_schema};
    drop table if exists {config.work_schema}.{table_name};
    create table {config.work_schema}.{table_name} as
    select
        subject_id,
        cohort_start_date,
        cohort_end_date
    from {config.cdm_schema}.cohort
    where cohort_definition_id = :cohort_definition_id
    order by cohort_start_date, subject_id
    """
    execute_sql(
        engine,
        materialize_sql,
        {"cohort_definition_id": int(cohort_definition_id)},
    )

    select_sql = f"""
    select *
    from {config.work_schema}.{table_name}
    order by cohort_start_date, subject_id
    """
    params = {}
    if limit is not None:
        select_sql += "\nlimit :limit"
        params["limit"] = int(limit)

    return read_sql_df(engine, select_sql, params)
