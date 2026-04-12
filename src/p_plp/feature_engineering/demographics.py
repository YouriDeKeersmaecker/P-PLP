# src/p_plp/feature_engineering/demographics.py

import pandas as pd

from p_plp.db import execute_sql, read_sql_df
from p_plp.db.config import get_engine_config
from p_plp.db.sql_utils import sql_age_expression


def build_demographic_features(engine) -> None:
    """
    Creates WORK_SCHEMA.demographic_features with one row per subject_id
    anchored on WORK_SCHEMA.labels (index_date).

    Columns:
      - subject_id
      - age (integer, age at index_date)
      - gender_concept_id
    """
    engine_config = get_engine_config(engine)
    cdm_schema = engine_config.cdm_schema
    work_schema = engine_config.work_schema
    age_sql = sql_age_expression("b.index_date")
    sql = f"""
    drop table if exists {work_schema}.demographic_features;

    create table {work_schema}.demographic_features as
    with base as (
        select
            l.subject_id,
            l.index_date
        from {work_schema}.labels l
    ),
    p as (
        select
            person_id,
            year_of_birth,
            month_of_birth,
            day_of_birth,
            gender_concept_id
        from {cdm_schema}.person
    )
    select
        b.subject_id,
        {age_sql} as age,
        p.gender_concept_id
    from base b
    join p
      on p.person_id = b.subject_id
    ;
    """
    execute_sql(engine, sql)


def get_demographic_features(engine) -> pd.DataFrame:
    work_schema = get_engine_config(engine).work_schema
    sql = f"""
    select *
    from {work_schema}.demographic_features
    order by subject_id
    """
    return read_sql_df(engine, sql)
