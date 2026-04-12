from p_plp.db import execute_sql, read_sql_df
from p_plp.db.sql_utils import sql_date_add_days
from p_plp.db.config import get_engine_config


def generate_labels_time_at_risk(
    engine,
    risk_start_days: int = 1,   # 1 = exclude same-day outcomes; 0 = include
    risk_end_days: int = 3365
):
    """Create the labels table by checking outcomes within the time-at-risk window."""

    work_schema = get_engine_config(engine).work_schema
    tar_start_sql = sql_date_add_days("t.index_date", "risk_start_days")
    tar_end_sql = sql_date_add_days("t.index_date", "risk_end_days")
    sql = f"""
    drop table if exists {work_schema}.labels;

    create table {work_schema}.labels as
    with t as (
        select
            subject_id,
            cohort_start_date as index_date
        from {work_schema}.target_cohort
    ),
    recurrent_outcomes as (
        -- Keep the first qualifying outcome within the risk window per subject.
        select
            t.subject_id,
            min(o.cohort_start_date) as outcome_date
        from t
        join {work_schema}.outcome_cohort o
          on o.subject_id = t.subject_id
         and o.cohort_start_date >= {tar_start_sql}
         and o.cohort_start_date <= {tar_end_sql}
        group by t.subject_id
    )
    select
        t.subject_id,
        t.index_date,
        {tar_start_sql} as tar_start_date,
        {tar_end_sql} as tar_end_date,
        case when ro.outcome_date is not null then 1 else 0 end as outcome_flag,
        ro.outcome_date
    from t
    left join recurrent_outcomes ro
      on ro.subject_id = t.subject_id;
    """

    execute_sql(engine, sql, {
        "risk_start_days": int(risk_start_days),
        "risk_end_days": int(risk_end_days),
    })

    select_sql = f"""
    select *
    from {work_schema}.labels
    order by index_date
    """

    return read_sql_df(engine, select_sql)
