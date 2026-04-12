import pandas as pd

from p_plp.db import execute_sql, read_sql_df
from p_plp.db.config import get_engine_config
from p_plp.db.sql_utils import sql_date_subtract_days


def build_prior_drug_count_features(engine, lookback_days: int = 365) -> None:
    """
    Creates WORK_SCHEMA.drug_count_features with one row per subject_id,
    counting drug_exposure rows in the lookback window:

      [index_date - lookback_days, index_date)

    i.e., strictly BEFORE index_date to avoid leakage.
    """
    engine_config = get_engine_config(engine)
    cdm_schema = engine_config.cdm_schema
    work_schema = engine_config.work_schema
    lookback_start_sql = sql_date_subtract_days("b.index_date", "lookback_days")
    sql = f"""
    drop table if exists {work_schema}.drug_count_features;

    create table {work_schema}.drug_count_features as
    with base as (
        select
            l.subject_id,
            l.index_date
        from {work_schema}.labels l
    ),
    drugs as (
        select
            de.person_id,
            de.drug_exposure_start_date
        from {cdm_schema}.drug_exposure de
        where de.drug_exposure_start_date is not null
    )
    select
        b.subject_id,
        coalesce(count(d.person_id), 0)::int as n_prior_drug_exposures
    from base b
    left join drugs d
      on d.person_id = b.subject_id
     and d.drug_exposure_start_date >= {lookback_start_sql}
     and d.drug_exposure_start_date <  b.index_date
    group by b.subject_id
    ;
    """
    execute_sql(engine, sql, {"lookback_days": int(lookback_days)})


def get_prior_drug_count_features(engine) -> pd.DataFrame:
    work_schema = get_engine_config(engine).work_schema
    sql = f"""
    select *
    from {work_schema}.drug_count_features
    order by subject_id
    """
    return read_sql_df(engine, sql)
