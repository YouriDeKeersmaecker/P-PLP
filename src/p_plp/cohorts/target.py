from sqlalchemy import text
from p_plp.db.utils import run_sql, fetch_df
from p_plp.db.config import CDM_SCHEMA, WORK_SCHEMA


def generate_target_cohort(engine, condition_concept_id: int):
    sql = f"""
    drop table if exists {WORK_SCHEMA}.target_cohort;

    create table {WORK_SCHEMA}.target_cohort as
    select
        person_id as subject_id,
        min(condition_start_date) as cohort_start_date,
        min(condition_start_date) as cohort_end_date
    from {CDM_SCHEMA}.condition_occurrence
    where condition_concept_id = :concept_id
    group by person_id;
    """

    run_sql(engine, sql, {"concept_id": condition_concept_id})

    select_sql = f"""
    select *
    from {WORK_SCHEMA}.target_cohort
    order by cohort_start_date
    """

    return fetch_df(engine, select_sql)