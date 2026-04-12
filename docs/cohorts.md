# Cohorts Layer

The `p_plp.cohorts` package contains the study-design part of the library.
It is responsible for turning a prediction question into concrete cohort tables and labels that the later feature-engineering and modeling steps can use.

In practice, this layer answers questions like:

- who enters the target cohort?
- what event counts as the outcome?
- which prediction problem types are supported?
- did the outcome happen inside the selected time-at-risk window?

The actual construction of a concrete `PredictionProblem` now lives in the builder layer.
So `p_plp.cohorts` owns the registry and metadata, while `p_plp.builders` turns that metadata plus user input into a runtime problem object.

## Folder structure

The package is split by responsibility:

- [`target.py`](d:/School/BachelorProef/P-PLP/src/p_plp/cohorts/target.py)
  - builds the `target_cohort` table
  - supports condition-based and visit-based target definitions
- [`outcome.py`](d:/School/BachelorProef/P-PLP/src/p_plp/cohorts/outcome.py)
  - builds the `outcome_cohort` table
  - supports condition-based and visit-based outcome definitions
- [`time_at_risk.py`](d:/School/BachelorProef/P-PLP/src/p_plp/cohorts/time_at_risk.py)
  - creates the `labels` table
  - checks whether outcomes occur inside the configured time-at-risk window
- [`problems.py`](d:/School/BachelorProef/P-PLP/src/p_plp/cohorts/problems.py)
  - defines the prediction-problem registry
  - resolves aliases and exposes lightweight metadata
- [`utils.py`](d:/School/BachelorProef/P-PLP/src/p_plp/cohorts/utils.py)
  - exploration helpers for conditions and candidate outcomes

Related builder:

- [`builders/problem.py`](d:/School/BachelorProef/P-PLP/src/p_plp/builders/problem.py)
  - builds the concrete `PredictionProblem` used by execution code
  - validates required fields and applies defaults from the registry

## Main public API

For most users, these are the main cohort-layer entry points:

```python
from p_plp.cohorts import (
    PredictionProblemDefinition,
    describe_problem_type,
    generate_labels_time_at_risk,
    generate_outcome_cohort,
    generate_target_cohort,
    list_observed_conditions,
    list_supported_problem_types,
    resolve_problem_definition,
)
```

These functions are useful both in notebooks and in step-by-step debugging workflows.

## Prediction problems

Prediction problems are defined through a registry rather than a growing chain of conditional logic.
Each registered problem type defines:

- canonical name
- supported aliases
- target kind
- outcome kind
- required fields
- default field values

Examples:

```python
from p_plp.cohorts import list_supported_problem_types, describe_problem_type

list_supported_problem_types()
describe_problem_type("condition_to_condition")
resolve_problem_definition("outpatient_revisit")
```

The registry metadata can then be turned into a concrete `PredictionProblem` by the builder layer:

```python
from p_plp.builders import build_prediction_problem

problem = build_prediction_problem(
    problem_type="condition_to_condition",
    target_concept_id=40481087,
    outcome_concept_id=257012,
)
```

If you already have a `PredictionProblemConfig`, you can also build through the higher-level helper:

```python
from p_plp.builders import build_problem
from p_plp.config import PredictionProblemConfig

problem = build_problem(
    PredictionProblemConfig(
        problem_type="condition_to_condition",
        target_concept_id=40481087,
        outcome_concept_id=257012,
    )
)
```

## Target cohort generation

`generate_target_cohort()` creates the `target_cohort` table in the work schema.
This table defines the index event for each included subject.

Examples:

```python
from p_plp.cohorts import generate_target_cohort

generate_target_cohort(
    engine,
    condition_concept_id=40481087,
    target_kind="condition",
)
```

```python
generate_target_cohort(
    engine,
    outpatient_visit_concept_id=9202,
    target_kind="visit",
)
```

For condition targets, the first qualifying condition occurrence per person is used as the index event.
For visit targets, the first qualifying visit per person is used.

## Outcome cohort generation

`generate_outcome_cohort()` creates the `outcome_cohort` table in the work schema.
This table contains the events that will later be matched against the time-at-risk window.

Example:

```python
from p_plp.cohorts import generate_outcome_cohort

generate_outcome_cohort(
    engine,
    outcome_concept_id=257012,
    outcome_kind="condition",
)
```

Visit-based outcomes are also supported for revisit-style problems.

## Time-at-risk and labels

`generate_labels_time_at_risk()` creates the `labels` table.
It joins the target cohort and outcome cohort and checks whether an outcome occurs between the configured start and end offsets after the index date.

Example:

```python
from p_plp.cohorts import generate_labels_time_at_risk

generate_labels_time_at_risk(
    engine,
    risk_start_days=1,
    risk_end_days=365,
)
```

The generated labels table includes:

- `subject_id`
- `index_date`
- `tar_start_date`
- `tar_end_date`
- `outcome_flag`
- `outcome_date`

## Exploration helpers

The exploration helpers in `utils.py` are useful for choosing target and outcome concepts before running the pipeline.

Examples:

```python
from p_plp.cohorts import (
    list_conditions_after_outpatient_visit,
    list_observed_conditions,
    list_observed_outcomes,
    list_observed_outpatient_conditions,
)

list_observed_conditions(engine, limit=10)
list_observed_outpatient_conditions(engine, limit=10)
list_conditions_after_outpatient_visit(engine, limit=10)
list_observed_outcomes(engine, limit=10)
```

These helpers are mainly intended for exploration and demonstration, not as the core execution API.

## API reference

### Public package

::: p_plp.cohorts

### Target module

::: p_plp.cohorts.target

### Outcome module

::: p_plp.cohorts.outcome

### Time-at-risk module

::: p_plp.cohorts.time_at_risk

### Problems module

::: p_plp.cohorts.problems

### Utilities module

::: p_plp.cohorts.utils
