from p_plp.db.utils import run_sql, fetch_df
from p_plp.db.config import CDM_SCHEMA, WORK_SCHEMA

def generate_labels_time_at_risk(
    engine,
    risk_start_days: int = 1,   # 1 = exclude same-day outcomes; 0 = include
    risk_end_days: int = 3365
):
    sql = f"""
    drop table if exists {WORK_SCHEMA}.labels;

    create table {WORK_SCHEMA}.labels as
    with t as (
        select
            subject_id,
            cohort_start_date as index_date
        from {WORK_SCHEMA}.target_cohort
    ),
    o as (
        select
            subject_id,
            cohort_start_date as outcome_date
        from {WORK_SCHEMA}.outcome_cohort
    )
    select
        t.subject_id,
        t.index_date,
        (t.index_date + (:risk_start_days || ' days')::interval)::date as tar_start_date,
        (t.index_date + (:risk_end_days   || ' days')::interval)::date as tar_end_date,
        case
            when o.outcome_date is not null
             and o.outcome_date >= (t.index_date + (:risk_start_days || ' days')::interval)::date
             and o.outcome_date <= (t.index_date + (:risk_end_days   || ' days')::interval)::date
            then 1 else 0
        end as outcome_flag,
        o.outcome_date
    from t
    left join o
      on o.subject_id = t.subject_id;
    """

    run_sql(engine, sql, {
        "risk_start_days": int(risk_start_days),
        "risk_end_days": int(risk_end_days),
    })

    select_sql = f"""
    select *
    from {WORK_SCHEMA}.labels
    order by index_date
    """

    return fetch_df(engine, select_sql)