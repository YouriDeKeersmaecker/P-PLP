import pandas as pd

from p_plp.db import get_engine_config, read_sql_df


def generate_feature_cte(engine, name, cfg):
    config = get_engine_config(engine)

    table = cfg["table"]
    concept_ids = ",".join(map(str, cfg["concept_ids"]))
    date_col = cfg["date_col"]
    concept_col = cfg["concept_col"]
    start, end = cfg["window"]

    return f"""
    {name} AS (
        SELECT
            c.subject_id,
            1 AS {name}
        FROM {config.work_schema}.labels c
        JOIN {config.cdm_schema}.{table} t
          ON c.subject_id = t.person_id
        JOIN {config.vocabulary_schema}.concept_ancestor ca
          ON t.{concept_col} = ca.descendant_concept_id
        WHERE ca.ancestor_concept_id IN ({concept_ids})
          AND t.{date_col} BETWEEN
              c.index_date + INTERVAL '{start} days'
              AND c.index_date + INTERVAL '{end} days'
        GROUP BY c.subject_id
    )
    """

def generate_demographic_cte(engine, name, cfg):
    config = get_engine_config(engine)

    if name == "age":
        return f"""
        {name} AS (
            SELECT
                c.subject_id,
                (c.index_date - MAKE_DATE(p.year_of_birth, p.month_of_birth, p.day_of_birth)) / 365.25 AS {name}
            FROM {config.work_schema}.labels c
            JOIN {config.cdm_schema}.person p
              ON c.subject_id = p.person_id
        )
        """
    if name == "gender":
        return f"""
        {name} AS (
            SELECT
                c.subject_id,
                p.gender_concept_id AS {name}
            FROM {config.work_schema}.labels c
            JOIN {config.cdm_schema}.person p
            ON c.subject_id = p.person_id
        )
        """

def build_full_query(engine, config, base_configs) -> str:
    engine_config = get_engine_config(engine)

    ctes = []
    joins = []
    select_cols = []

    for name, cfg in config.items():
        if cfg.get("type") == "demographic":
            ctes.append(generate_demographic_cte(engine, name, cfg))
            select_cols.append(f"{name}.{name} AS {name}")
        else:
            resolved_cfg = {
                **base_configs[cfg["base"]],
                **cfg,
            }
            resolved_cfg.pop("base", None)

            ctes.append(generate_feature_cte(engine, name, resolved_cfg))
            select_cols.append(f"COALESCE({name}.{name}, 0) AS {name}")

        joins.append(f"LEFT JOIN {name} USING (subject_id)")

    cte_sql = ",\n".join(ctes)

    final_sql = f"""
    WITH
    {cte_sql}

    SELECT 
        c.*,
        {', '.join(select_cols)}
    FROM {engine_config.work_schema}.labels c
    {' '.join(joins)}
    """

    return final_sql


def run_feature_query(engine, config, base_config) -> pd.DataFrame:
    """Build and execute the feature query, returning the result as a DataFrame."""

    sql = build_full_query(engine, config, base_config)
    return read_sql_df(engine, sql)
