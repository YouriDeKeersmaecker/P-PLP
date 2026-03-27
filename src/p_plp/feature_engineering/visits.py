import pandas as pd

from p_plp.db import fetch_df, run_sql
from p_plp.db.config import CDM_SCHEMA, WORK_SCHEMA


def build_prior_visit_count_features(engine, lookback_days: int = 365) -> None:
    """
    Creates WORK_SCHEMA.visit_count_features with one row per subject_id,
    counting visit_occurrence rows in the lookback window:

      [index_date - lookback_days, index_date)

    i.e., strictly BEFORE index_date to avoid leakage.
    """
    sql = f"""
    drop table if exists {WORK_SCHEMA}.visit_count_features;

    create table {WORK_SCHEMA}.visit_count_features as
    with base as (
        select
            l.subject_id,
            l.index_date
        from {WORK_SCHEMA}.labels l
    ),
    visits as (
        select
            vo.person_id,
            vo.visit_start_date
        from {CDM_SCHEMA}.visit_occurrence vo
        where vo.visit_start_date is not null
    )
    select
        b.subject_id,
        coalesce(count(v.person_id), 0)::int as n_prior_visits
    from base b
    left join visits v
      on v.person_id = b.subject_id
     and v.visit_start_date >= (b.index_date - (:lookback_days || ' days')::interval)::date
     and v.visit_start_date <  b.index_date
    group by b.subject_id
    ;
    """
    run_sql(engine, sql, {"lookback_days": int(lookback_days)})


def get_prior_visit_count_features(engine) -> pd.DataFrame:
    sql = f"""
    select *
    from {WORK_SCHEMA}.visit_count_features
    order by subject_id
    """
    return fetch_df(engine, sql)
