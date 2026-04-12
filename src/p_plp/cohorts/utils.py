import pandas as pd
from p_plp.db import read_sql_df
from p_plp.db.config import get_engine_config


def list_observed_conditions(engine, search: str | None = None, limit: int = 20):
    """List the most frequently observed condition concepts in the current CDM source."""

    cdm_schema = get_engine_config(engine).cdm_schema
    sql = f"""
    select
        c.concept_id,
        c.concept_name,
        c.vocabulary_id,
        count(*) as n_occurrences
    from {cdm_schema}.condition_occurrence co
    join {cdm_schema}.concept c
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

    return read_sql_df(engine, sql, params)


def list_observed_outpatient_conditions(
    engine,
    search: str | None = None,
    limit: int = 20,
    outpatient_visit_concept_id: int = 9202,
):
    """List condition concepts observed during visits matching the given visit concept."""

    cdm_schema = get_engine_config(engine).cdm_schema
    sql = f"""
    select
        c.concept_id,
        c.concept_name,
        c.vocabulary_id,
        count(*) as n_outpatient_occurrences
    from {cdm_schema}.condition_occurrence co
    join {cdm_schema}.visit_occurrence vo
        on vo.visit_occurrence_id = co.visit_occurrence_id
    join {cdm_schema}.concept c
        on c.concept_id = co.condition_concept_id
    where vo.visit_concept_id = :outpatient_visit_concept_id
    """

    params = {
        "limit": int(limit),
        "outpatient_visit_concept_id": int(outpatient_visit_concept_id),
    }

    if search:
        sql += " and lower(c.concept_name) like lower(:search)"
        params["search"] = f"%{search}%"

    sql += """
    group by c.concept_id, c.concept_name, c.vocabulary_id
    order by n_outpatient_occurrences desc
    limit :limit
    """

    return read_sql_df(engine, sql, params)


def list_conditions_after_outpatient_visit(
    engine,
    risk_start_days: int = 1,
    risk_end_days: int = 90,
    limit: int = 20,
    outpatient_visit_concept_id: int = 9202,
    search: str | None = None,
):
    """Explore candidate outcome conditions after the first qualifying outpatient visit."""

    cdm_schema = get_engine_config(engine).cdm_schema
    sql = f"""
    with first_outpatient_visit as (
        select
            vo.person_id,
            min(vo.visit_start_date) as index_date
        from {cdm_schema}.visit_occurrence vo
        where vo.visit_concept_id = :outpatient_visit_concept_id
          and vo.visit_start_date is not null
        group by vo.person_id
    )
    select
        c.concept_id,
        c.concept_name,
        c.vocabulary_id,
        count(*) as n_occurrences_within_window,
        count(distinct co.person_id) as n_people
    from first_outpatient_visit i
    join {cdm_schema}.condition_occurrence co
      on co.person_id = i.person_id
     and co.condition_start_date >= (i.index_date + :risk_start_days)
     and co.condition_start_date <= (i.index_date + :risk_end_days)
    join {cdm_schema}.concept c
      on c.concept_id = co.condition_concept_id
    where co.condition_start_date is not null
    """

    params = {
        "outpatient_visit_concept_id": int(outpatient_visit_concept_id),
        "risk_start_days": int(risk_start_days),
        "risk_end_days": int(risk_end_days),
        "limit": int(limit),
    }

    if search:
        sql += " and lower(c.concept_name) like lower(:search)"
        params["search"] = f"%{search}%"

    sql += """
    group by c.concept_id, c.concept_name, c.vocabulary_id
    order by n_occurrences_within_window desc, n_people desc
    limit :limit
    """

    return read_sql_df(engine, sql, params)


def list_observed_outcomes(engine, search: str | None = None, limit: int = 20):
    """List common standard outcome concepts from condition occurrence data."""

    cdm_schema = get_engine_config(engine).cdm_schema
    sql = f"""
    select
        c.concept_id,
        c.concept_name,
        c.vocabulary_id,
        count(*) as n_occurrences
    from {cdm_schema}.condition_occurrence co
    join {cdm_schema}.concept c
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

    return read_sql_df(engine, sql, params)
