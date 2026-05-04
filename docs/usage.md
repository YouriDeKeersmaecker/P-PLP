# Usage

P-PLP is used through its subpackages rather than a single top-level runner.

## 1. Create an engine

PostgreSQL:

```python
from p_plp.db import get_engine

engine = get_engine(
    source_name="postgres",
    database_url="postgresql+psycopg2://user:password@localhost:5432/db",
    cdm_schema="cdm",
    work_schema="plp_work",
)
```

DuckDB / Eunomia:

```python
from p_plp.db import get_engine

engine = get_engine(
    source_name="eunomia",
    database_path=r"C:\path\to\eunomia.duckdb",
    cdm_schema="main",
    work_schema="plp_work",
)
```

## 2. Load target and outcome cohorts

```python
from p_plp.cohorts import load_atlas_cohort_to_work_table

load_atlas_cohort_to_work_table(
    engine,
    sql=target_sql,
    cohort_definition_id=1,
    table_name="target_cohort",
)

load_atlas_cohort_to_work_table(
    engine,
    sql=outcome_sql,
    cohort_definition_id=2,
    table_name="outcome_cohort",
)
```

This creates `target_cohort` and `outcome_cohort` in the configured work schema.

## 3. Generate labels

```python
from p_plp.cohorts import generate_labels_time_at_risk

labels_df = generate_labels_time_at_risk(
    engine,
    risk_start_days=1,
    risk_end_days=365,
)
```

## 4. Build features

```python
from p_plp.feature_engineering import create_covariate_settings, run_feature_query

feature_config, base_config = create_covariate_settings(
    engine,
    useDemographicsAge=True,
    useDemographicsGender=True,
    useConditionEraAnyTimePrior=True,
    useObservationAnyTimePrior=True,
    min_count=5,
)

dataset_df = run_feature_query(engine, feature_config, base_config)
```

## 5. Train and evaluate

```python
from p_plp.modeling import train_pipeline, evaluate

model, X_test, y_test = train_pipeline(dataset_df, model_name="logreg")
metrics = evaluate(model, X_test, y_test)
```

## Optional helpers

Database helpers:

```python
from p_plp.db import list_cdm_tables, read_table, get_engine_config

cdm_tables = list_cdm_tables(engine)
person_df = read_table(engine, "person", schema=get_engine_config(engine).cdm_schema)
```

Cohort exploration helpers:

```python
from p_plp.cohorts import list_observed_conditions, list_observed_outcomes

conditions = list_observed_conditions(engine, limit=10)
outcomes = list_observed_outcomes(engine, limit=10)
```
