# src/p_plp/feature_engineering/demographics.py

import pandas as pd

from p_plp.db import *
from p_plp.db.config import CDM_SCHEMA, WORK_SCHEMA


def build_demographic_features(engine) -> None:
    """
    Creates WORK_SCHEMA.demographic_features with one row per subject_id
    anchored on WORK_SCHEMA.labels (index_date).

    Columns:
      - subject_id
      - age (integer, age at index_date)
      - gender_concept_id
    """
    sql = f"""
    drop table if exists {WORK_SCHEMA}.demographic_features;

    create table {WORK_SCHEMA}.demographic_features as
    with base as (
        select
            l.subject_id,
            l.index_date
        from {WORK_SCHEMA}.labels l
    ),
    p as (
        select
            person_id,
            year_of_birth,
            month_of_birth,
            day_of_birth,
            gender_concept_id
        from {CDM_SCHEMA}.person
    )
    select
        b.subject_id,
        -- Build a birthdate; if month/day are missing, default to mid-year/mid-month
        extract(
            year from age(
                b.index_date,
                make_date(
                    p.year_of_birth,
                    coalesce(nullif(p.month_of_birth, 0), 6),
                    coalesce(nullif(p.day_of_birth, 0), 15)
                )
            )
        )::int as age,
        p.gender_concept_id
    from base b
    join p
      on p.person_id = b.subject_id
    ;
    """
    run_sql(engine, sql)


def get_demographic_features(engine) -> pd.DataFrame:
    sql = f"""
    select *
    from {WORK_SCHEMA}.demographic_features
    order by subject_id
    """
    return fetch_df(engine, sql)