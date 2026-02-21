import pandas as pd
from p_plp.db import *
from p_plp.db.config import WORK_SCHEMA


def build_model_dataset(engine) -> None:
    """
    Creates WORK_SCHEMA.model_dataset as the final wide table for modeling.

    Expected inputs (already created):
      - WORK_SCHEMA.labels (subject_id, index_date, outcome_flag, ...)
      - WORK_SCHEMA.demographic_features (subject_id, age, gender_concept_id)
      - WORK_SCHEMA.condition_count_features (subject_id, n_prior_conditions)

    Output columns:
      - subject_id
      - index_date
      - age
      - gender_concept_id
      - n_prior_conditions
      - outcome_flag
    """
    sql = f"""
    drop table if exists {WORK_SCHEMA}.model_dataset;

    create table {WORK_SCHEMA}.model_dataset as
    select
        l.subject_id,
        l.index_date,
        d.age,
        d.gender_concept_id,
        coalesce(c.n_prior_conditions, 0)::int as n_prior_conditions,
        l.outcome_flag
    from {WORK_SCHEMA}.labels l
    left join {WORK_SCHEMA}.demographic_features d
      on d.subject_id = l.subject_id
    left join {WORK_SCHEMA}.condition_count_features c
      on c.subject_id = l.subject_id
    ;
    """
    run_sql(engine, sql)


def get_model_dataset(engine) -> pd.DataFrame:
    sql = f"""
    select *
    from {WORK_SCHEMA}.model_dataset
    order by index_date, subject_id
    """
    return fetch_df(engine, sql)