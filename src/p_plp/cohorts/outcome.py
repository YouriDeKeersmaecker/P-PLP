from p_plp.db.utils import run_sql, fetch_df
from p_plp.db.config import CDM_SCHEMA, WORK_SCHEMA


def generate_outcome_cohort(engine, outcome_concept_id: int, limit: int | None = None):
    create_sql = f"""
    drop table if exists {WORK_SCHEMA}.outcome_cohort;

    create table {WORK_SCHEMA}.outcome_cohort as
    select
        person_id as subject_id,
        min(condition_start_date) as cohort_start_date,
        min(condition_start_date) as cohort_end_date
    from {CDM_SCHEMA}.condition_occurrence
    where condition_concept_id = :concept_id
    group by person_id;
    """

    run_sql(engine, create_sql, {"concept_id": outcome_concept_id})

    select_sql = f"""
    select *
    from {WORK_SCHEMA}.outcome_cohort
    order by cohort_start_date
    """

    return fetch_df(engine, select_sql)