from enum import Enum

import pandas as pd
from sqlalchemy import text

from .config import get_engine_config


class CdmTable(str, Enum):
    """Common OMOP CDM tables exposed as enum values for convenience."""

    PERSON = "person"
    CONDITION_OCCURRENCE = "condition_occurrence"
    DRUG_EXPOSURE = "drug_exposure"
    VISIT_OCCURRENCE = "visit_occurrence"
    OBSERVATION_PERIOD = "observation_period"
    MEASUREMENT = "measurement"
    PROCEDURE_OCCURRENCE = "procedure_occurrence"
    DEATH = "death"


def execute_sql(engine, sql: str, params: dict | None = None) -> None:
    """Execute a SQL statement inside a transaction."""

    with engine.begin() as conn:
        if params:
            conn.execute(text(sql), params)
        else:
            conn.exec_driver_sql(sql)


def read_sql_df(engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    """Execute a SQL query and return the result as a pandas DataFrame."""

    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def read_table(
    engine,
    table_name: str,
    schema: str | None = None,
    limit: int | None = 20,
) -> pd.DataFrame:
    """Read an arbitrary table from the given schema into a pandas DataFrame."""

    config = get_engine_config(engine)
    resolved_schema = schema or (config.cdm_schema if config is not None else None)
    if resolved_schema is None:
        raise ValueError("No schema provided and no engine configuration is attached.")
    resolved_table_name = table_name.value if isinstance(table_name, CdmTable) else str(table_name)
    sql = f"select * from {resolved_schema}.{resolved_table_name}"
    if limit is not None:
        sql += " limit :limit"
        return read_sql_df(engine, sql, {"limit": int(limit)})
    return read_sql_df(engine, sql)


def _list_tables(engine, schema: str) -> pd.DataFrame:
    """List base tables available in a given schema."""

    sql = """
    select
        table_name
    from information_schema.tables
    where table_schema = :schema
      and table_type = 'BASE TABLE'
    order by table_name
    """
    return read_sql_df(engine, sql, {"schema": schema})


def list_cdm_tables(engine) -> pd.DataFrame:
    """List the base tables available in the configured CDM schema."""

    config = get_engine_config(engine)
    if config is None:
        raise ValueError("No engine configuration is attached.")
    return _list_tables(engine, config.cdm_schema)
