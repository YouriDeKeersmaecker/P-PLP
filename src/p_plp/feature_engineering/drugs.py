import pandas as pd

from p_plp.db import fetch_df, run_sql
from p_plp.db.config import CDM_SCHEMA, WORK_SCHEMA


def build_prior_drug_count_features(engine, lookback_days: int = 365) -> None:
    """
    Creates WORK_SCHEMA.drug_count_features with one row per subject_id,
    counting drug_exposure rows in the lookback window:

      [index_date - lookback_days, index_date)

    i.e., strictly BEFORE index_date to avoid leakage.
    """
    sql = f"""
    drop table if exists {WORK_SCHEMA}.drug_count_features;

    create table {WORK_SCHEMA}.drug_count_features as
    with base as (
        select
            l.subject_id,
            l.index_date
        from {WORK_SCHEMA}.labels l
    ),
    drugs as (
        select
            de.person_id,
            de.drug_exposure_start_date
        from {CDM_SCHEMA}.drug_exposure de
        where de.drug_exposure_start_date is not null
    )
    select
        b.subject_id,
        coalesce(count(d.person_id), 0)::int as n_prior_drug_exposures
    from base b
    left join drugs d
      on d.person_id = b.subject_id
     and d.drug_exposure_start_date >= (b.index_date - (:lookback_days || ' days')::interval)::date
     and d.drug_exposure_start_date <  b.index_date
    group by b.subject_id
    ;
    """
    run_sql(engine, sql, {"lookback_days": int(lookback_days)})


def get_prior_drug_count_features(engine) -> pd.DataFrame:
    sql = f"""
    select *
    from {WORK_SCHEMA}.drug_count_features
    order by subject_id
    """
    return fetch_df(engine, sql)
