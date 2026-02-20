import pandas as pd
from sqlalchemy import text
from enum import Enum
from .config import CDM_SCHEMA


class CdmTable(str, Enum):
    PERSON = "person"
    CONDITION_OCCURRENCE = "condition_occurrence"
    DRUG_EXPOSURE = "drug_exposure"
    VISIT_OCCURRENCE = "visit_occurrence"
    OBSERVATION_PERIOD = "observation_period"
    MEASUREMENT = "measurement"
    PROCEDURE_OCCURRENCE = "procedure_occurrence"
    DEATH = "death"


def run_sql(engine, sql: str, params: dict | None = None) -> None:
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


def fetch_df(engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def get_cdm_table(engine, table: CdmTable, limit: int | None = 20) -> pd.DataFrame:
    sql = f"select * from {CDM_SCHEMA}.{table.value}"
    if limit is not None:
        sql += " limit :limit"
        return fetch_df(engine, sql, {"limit": int(limit)})
    return fetch_df(engine, sql)

def list_cdm_tables(engine) -> pd.DataFrame:
    sql = """
    select
        table_name
    from information_schema.tables
    where table_schema = :schema
      and table_type = 'BASE TABLE'
    order by table_name
    """

    return fetch_df(engine, sql, {"schema": CDM_SCHEMA})


def list_conditions(engine, search: str | None = None, limit: int = 50) -> pd.DataFrame:
    sql = f"""
    select
        c.concept_id,
        c.concept_name,
        c.vocabulary_id,
        c.domain_id
    from {CDM_SCHEMA}.concept c
    where c.domain_id = 'Condition'
    """

    params: dict = {"limit": int(limit)}

    if search:
        sql += " and lower(c.concept_name) like lower(:search)"
        params["search"] = f"%{search}%"

    sql += " order by c.concept_name limit :limit"

    return fetch_df(engine, sql, params)