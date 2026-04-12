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


def read_cohort_table(
    engine,
    cohort_definition_id: int,
    limit: int | None = None,
):
    """Backward-compatible alias for reading generated Atlas cohorts."""

    return read_atlas_cohort(
        engine,
        cohort_definition_id=cohort_definition_id,
        limit=limit,
    )


def load_cohort_from_sql(
    engine,
    sql: str,
    cohort_definition_id: int,
    limit: int | None = None,
):
    """Backward-compatible alias for executing and reading Atlas cohorts."""

    return load_atlas_cohort(
        engine,
        sql=sql,
        cohort_definition_id=cohort_definition_id,
        limit=limit,
    )
