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


def build_full_query(engine, config) -> str:
    engine_config = get_engine_config(engine)

    ctes = []
    joins = []

    for name, cfg in config.items():
        ctes.append(generate_feature_cte(engine, name, cfg))
        joins.append(f"LEFT JOIN {name} USING (subject_id)")

    cte_sql = ",\n".join(ctes)

    final_sql = f"""
    WITH
    {cte_sql}

    SELECT 
        c.*,
        {', '.join([f'COALESCE({name}.{name}, 0) AS {name}' for name in config])}
    FROM {engine_config.work_schema}.labels c
    {' '.join(joins)}
    """

    return run_feature_query(engine, final_sql)


def run_feature_query(engine, config) -> pd.DataFrame:
    """Build and execute the feature query, returning the result as a DataFrame."""

    sql = build_full_query(engine, config)
    return read_sql_df(engine, sql)

