# Cohorts Layer

`p_plp.cohorts` contains helpers for loading cohorts and generating labels.

Main tasks:

- run Atlas cohort SQL
- read cohort rows from the OMOP `cohort` table
- copy target and outcome cohorts into the work schema
- generate `labels` for a time-at-risk window
- inspect observed conditions and outcomes

## Main functions

```python
from p_plp.cohorts import (
    execute_atlas_sql,
    generate_labels_time_at_risk,
    list_conditions_after_outpatient_visit,
    list_observed_conditions,
    list_observed_outcomes,
    list_observed_outpatient_conditions,
    load_atlas_cohort,
    load_atlas_cohort_to_work_table,
    read_atlas_cohort,
)
```

## Load a cohort into the OMOP cohort table

```python
from p_plp.cohorts import load_atlas_cohort

target_df = load_atlas_cohort(
    engine,
    sql=target_sql,
    cohort_definition_id=1,
)
```

## Copy a cohort to the work schema

```python
from p_plp.cohorts import load_atlas_cohort_to_work_table

load_atlas_cohort_to_work_table(
    engine,
    sql=target_sql,
    cohort_definition_id=1,
    table_name="target_cohort",
)
```

Typical table names are `target_cohort` and `outcome_cohort`.

## Generate labels

```python
from p_plp.cohorts import generate_labels_time_at_risk

labels_df = generate_labels_time_at_risk(
    engine,
    risk_start_days=1,
    risk_end_days=365,
)
```

This reads from `target_cohort` and `outcome_cohort` in the work schema and creates `labels`.

## Explore observed concepts

```python
from p_plp.cohorts import (
    list_conditions_after_outpatient_visit,
    list_observed_conditions,
    list_observed_outcomes,
    list_observed_outpatient_conditions,
)

conditions = list_observed_conditions(engine, limit=10)
outcomes = list_observed_outcomes(engine, limit=10)
```

## API reference

::: p_plp.cohorts

::: p_plp.cohorts.load_cohorts

::: p_plp.cohorts.labels

::: p_plp.cohorts.utils
