import pandas as pd
from p_plp.db import execute_sql, read_sql_df
from p_plp.db.config import get_engine_config


def build_model_dataset(engine) -> None:
    """
    Creates WORK_SCHEMA.model_dataset as the final wide table for modeling.

    Expected inputs (already created):
      - WORK_SCHEMA.labels (subject_id, index_date, outcome_flag, ...)
      - WORK_SCHEMA.demographic_features (subject_id, age, gender_concept_id)
      - WORK_SCHEMA.condition_count_features (subject_id, n_prior_conditions)
      - WORK_SCHEMA.drug_count_features (subject_id, n_prior_drug_exposures)
      - WORK_SCHEMA.visit_count_features (subject_id, n_prior_visits)

    Output columns:
      - subject_id
      - index_date
      - age
      - gender_concept_id
      - n_prior_conditions
      - n_prior_drug_exposures
      - n_prior_visits
      - outcome_flag
    """
    work_schema = get_engine_config(engine).work_schema
    sql = f"""
    drop table if exists {work_schema}.model_dataset;

    create table {work_schema}.model_dataset as
    select
        l.subject_id,
        l.index_date,
        d.age,
        d.gender_concept_id,
        coalesce(c.n_prior_conditions, 0)::int as n_prior_conditions,
        coalesce(dr.n_prior_drug_exposures, 0)::int as n_prior_drug_exposures,
        coalesce(v.n_prior_visits, 0)::int as n_prior_visits,
        l.outcome_flag
    from {work_schema}.labels l
    left join {work_schema}.demographic_features d
      on d.subject_id = l.subject_id
    left join {work_schema}.condition_count_features c
      on c.subject_id = l.subject_id
    left join {work_schema}.drug_count_features dr
      on dr.subject_id = l.subject_id
    left join {work_schema}.visit_count_features v
      on v.subject_id = l.subject_id
    ;
    """
    execute_sql(engine, sql)


def get_model_dataset(engine) -> pd.DataFrame:
    work_schema = get_engine_config(engine).work_schema
    sql = f"""
    select *
    from {work_schema}.model_dataset
    order by index_date, subject_id
    """
    return read_sql_df(engine, sql)
