import pandas as pd

from p_plp.db import get_engine_config, read_sql_df

DEFAULT_COVARIATE_BASE_CONFIGS = {
    "condition_era_any_time_prior": {
        "table": "condition_era",
        "date_col": "condition_era_start_date",
        "concept_col": "condition_concept_id",
        "window": (-9999, 0),
    },
    "observation_any_time_prior": {
        "table": "observation",
        "date_col": "observation_date",
        "concept_col": "observation_concept_id",
        "window": (-9999, 0),
    },
    "drug_era_any_time_prior": {
        "table": "drug_era",
        "date_col": "drug_era_start_date",
        "concept_col": "drug_concept_id",
        "window": (-9999, 0),
    },
}

NON_FEATURE_COLUMNS = [
    "subject_id",
    "index_date",
    "tar_start_date",
    "tar_end_date",
    "outcome_date",
]


def drop_non_feature_columns(
    df: pd.DataFrame,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    cols_to_drop = columns if columns is not None else NON_FEATURE_COLUMNS
    return df.drop(columns=[col for col in cols_to_drop if col in df.columns])


def _get_distinct_concept_ids(
    engine,
    cfg: dict,
    min_count: int = 1,
) -> list[int]:
    engine_config = get_engine_config(engine)
    table = cfg["table"]
    concept_col = cfg["concept_col"]

    if int(min_count) < 1:
        raise ValueError("min_count must be >= 1")

    sql = f"""
    SELECT t.{concept_col} AS concept_id
    FROM {engine_config.work_schema}.labels c
    JOIN {engine_config.cdm_schema}.{table} t
      ON c.subject_id = t.person_id
    WHERE t.{concept_col} IS NOT NULL
    GROUP BY t.{concept_col}
    HAVING COUNT(DISTINCT c.subject_id) >= {int(min_count)}
    ORDER BY t.{concept_col}
    """

    concept_df = read_sql_df(engine, sql)
    if concept_df.empty:
        return []

    return [int(concept_id) for concept_id in concept_df["concept_id"].tolist()]


def create_covariate_settings(
    engine,
    *,
    useDemographicsGender: bool = False,
    useDemographicsAge: bool = False,
    useConditionEraAnyTimePrior: bool = False,
    useObservationAnyTimePrior: bool = False,
    useDrugEraAnyTimePrior: bool = False,
    min_count: int = 1,
):
    """Create a feature config from OHDSI-style covariate flags.

    Returns a tuple of (config, base_config) that can be passed to
    run_feature_query(engine, config, base_config).
    """

    config: dict[str, dict] = {}
    base_config: dict[str, dict] = {}

    if useDemographicsGender:
        config["gender"] = {"type": "demographic"}

    if useDemographicsAge:
        config["age"] = {"type": "demographic"}

    domain_flags = [
        ("condition_era_any_time_prior", useConditionEraAnyTimePrior, "condition_era"),
        ("observation_any_time_prior", useObservationAnyTimePrior, "observation"),
        ("drug_era_any_time_prior", useDrugEraAnyTimePrior, "drug_era"),
    ]

    for base_name, enabled, feature_prefix in domain_flags:
        if not enabled:
            continue

        base_cfg = DEFAULT_COVARIATE_BASE_CONFIGS[base_name]
        base_config[base_name] = base_cfg

        for concept_id in _get_distinct_concept_ids(
            engine,
            base_cfg,
            min_count=min_count,
        ):
            feature_name = f"{feature_prefix}_{concept_id}"
            config[feature_name] = {
                "base": base_name,
                "concept_ids": [concept_id],
            }

    return config, base_config


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
        WHERE t.{concept_col} IN ({concept_ids})
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
    if name == "hospitalizations_count":
        return f"""
        {name} AS (
            SELECT
                c.subject_id,
                COUNT(vo.visit_occurrence_id) AS {name}
            FROM {config.work_schema}.labels c
            LEFT JOIN {config.cdm_schema}.visit_occurrence vo
              ON c.subject_id = vo.person_id
             AND vo.visit_start_date <= c.index_date
            GROUP BY c.subject_id
        )
        """

def generate_categorical_feature_cte(engine, name, cfg):
    config = get_engine_config(engine)

    table = cfg["table"]
    concept_ids = ",".join(map(str, cfg["concept_ids"]))
    date_col = cfg["date_col"]
    concept_col = cfg["concept_col"]
    value_col = cfg["value_col"]
    start, end = cfg["window"]

    case_clauses = []
    for label, ids in cfg["value_map"].items():
        value_ids = ",".join(map(str, ids))
        case_clauses.append(
            f"WHEN t.{value_col} IN ({value_ids}) THEN '{label}'"
        )

    case_sql = "\n                ".join(case_clauses)

    return f"""
    {name} AS (
        SELECT
            subject_id,
            {name}
        FROM (
            SELECT
                c.subject_id,
                CASE
                    {case_sql}
                    ELSE NULL
                END AS {name},
                ROW_NUMBER() OVER (
                    PARTITION BY c.subject_id
                    ORDER BY t.{date_col} DESC
                ) AS rn
            FROM {config.work_schema}.labels c
            JOIN {config.cdm_schema}.{table} t
              ON c.subject_id = t.person_id
            WHERE t.{concept_col} IN ({concept_ids})
              AND t.{date_col} BETWEEN
                  c.index_date + INTERVAL '{start} days'
                  AND c.index_date + INTERVAL '{end} days'
        ) ranked
        WHERE rn = 1
          AND {name} IS NOT NULL
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

            if "value_map" in resolved_cfg:
                ctes.append(generate_categorical_feature_cte(engine, name, resolved_cfg))
                select_cols.append(f"{name}.{name} AS {name}")
            else:
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
    full_labels_df = read_sql_df(engine, sql)
    return drop_non_feature_columns(full_labels_df, NON_FEATURE_COLUMNS)
