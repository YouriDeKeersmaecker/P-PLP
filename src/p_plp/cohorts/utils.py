import pandas as pd
from p_plp.db.config import CDM_SCHEMA, WORK_SCHEMA
from p_plp.db.utils import run_sql, fetch_df

def list_observed_conditions(engine, search: str | None = None, limit: int = 20):
    sql = f"""
    select
        c.concept_id,
        c.concept_name,
        c.vocabulary_id,
        count(*) as n_occurrences
    from {CDM_SCHEMA}.condition_occurrence co
    join {CDM_SCHEMA}.concept c
        on c.concept_id = co.condition_concept_id
    where 1=1
    """

    params = {"limit": int(limit)}

    if search:
        sql += " and lower(c.concept_name) like lower(:search)"
        params["search"] = f"%{search}%"

    sql += """
    group by c.concept_id, c.concept_name, c.vocabulary_id
    order by n_occurrences desc
    limit :limit
    """

    return fetch_df(engine, sql, params)

def list_observed_outcomes(engine, search: str | None = None, limit: int = 20):
    sql = f"""
    select
        c.concept_id,
        c.concept_name,
        c.vocabulary_id,
        count(*) as n_occurrences
    from {CDM_SCHEMA}.condition_occurrence co
    join {CDM_SCHEMA}.concept c
        on c.concept_id = co.condition_concept_id
    where c.standard_concept = 'S'
    """

    params = {"limit": int(limit)}

    if search:
        sql += " and lower(c.concept_name) like lower(:search)"
        params["search"] = f"%{search}%"

    sql += """
    group by c.concept_id, c.concept_name, c.vocabulary_id
    order by n_occurrences desc
    limit :limit
    """

    return fetch_df(engine, sql, params)