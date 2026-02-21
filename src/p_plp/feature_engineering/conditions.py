# src/p_plp/feature_engineering/conditions.py

import pandas as pd

from p_plp.db import fetch_df, run_sql
from p_plp.db.config import CDM_SCHEMA, WORK_SCHEMA


def build_prior_condition_count_features(engine, lookback_days: int = 365) -> None:
    """
    Creates WORK_SCHEMA.condition_count_features with one row per subject_id,
    counting condition_occurrence rows in the lookback window:

      [index_date - lookback_days, index_date)

    i.e., strictly BEFORE index_date to avoid leakage.
    """
    sql = f"""
    drop table if exists {WORK_SCHEMA}.condition_count_features;

    create table {WORK_SCHEMA}.condition_count_features as
    with base as (
        select
            l.subject_id,
            l.index_date
        from {WORK_SCHEMA}.labels l
    ),
    conds as (
        select
            co.person_id,
            co.condition_start_date
        from {CDM_SCHEMA}.condition_occurrence co
        where co.condition_start_date is not null
    )
    select
        b.subject_id,
        coalesce(count(c.person_id), 0)::int as n_prior_conditions
    from base b
    left join conds c
      on c.person_id = b.subject_id
     and c.condition_start_date >= (b.index_date - (:lookback_days || ' days')::interval)::date
     and c.condition_start_date <  b.index_date
    group by b.subject_id
    ;
    """
    run_sql(engine, sql, {"lookback_days": int(lookback_days)})


def get_prior_condition_count_features(engine) -> pd.DataFrame:
    sql = f"""
    select *
    from {WORK_SCHEMA}.condition_count_features
    order by subject_id
    """
    return fetch_df(engine, sql)