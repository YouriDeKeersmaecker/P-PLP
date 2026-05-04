# P-PLP

P-PLP is a Python package for patient-level prediction on OMOP CDM data.

It helps you:

- connect to PostgreSQL or DuckDB / Eunomia-style OMOP datasets
- load target and outcome cohorts from Atlas SQL
- generate labels for a time-at-risk window
- build feature datasets
- train and evaluate prediction models

## Installation

Install from PyPI:

```bash
pip install p_plp
```

Install locally for development:

```bash
pip install -e .[dev]
```

## Quick example

```python
from p_plp.db import get_engine
from p_plp.cohorts import load_atlas_cohort_to_work_table, generate_labels_time_at_risk
from p_plp.feature_engineering import create_covariate_settings, run_feature_query
from p_plp.modeling import train_pipeline, evaluate

engine = get_engine(
    source_name="postgres",
    database_url="postgresql+psycopg2://user:password@localhost:5432/db",
    cdm_schema="cdm",
    work_schema="plp_work",
)

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

labels_df = generate_labels_time_at_risk(engine, risk_start_days=1, risk_end_days=365)

feature_config, base_config = create_covariate_settings(
    engine,
    useDemographicsAge=True,
    useDemographicsGender=True,
    useConditionEraAnyTimePrior=True,
)

dataset_df = run_feature_query(engine, feature_config, base_config)
model, X_test, y_test = train_pipeline(dataset_df, model_name="logreg")
metrics = evaluate(model, X_test, y_test)
```

## More

- Docs: [docs/](docs/)
- Usage guide: [docs/usage.md](docs/usage.md)
- API reference: [docs/api.md](docs/api.md)
- Notebooks and examples: [notebooks/](notebooks/)
