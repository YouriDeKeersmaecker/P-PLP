# Usage

## Function-based API

```python
from p_plp import run_postgres_pipeline

result = run_postgres_pipeline(
    target_concept_id=40481087,
    outcome_concept_id=133228,
)
```

## Spec-based API

```python
from p_plp import PipelineRunConfig, PredictionProblemConfig, run_pipeline_config

config = PipelineRunConfig(
    source_name="postgres",
    problem=PredictionProblemConfig(
        problem_type="condition_to_condition",
        target_concept_id=40481087,
        outcome_concept_id=133228,
    ),
)

result = run_pipeline_config(config)
```

## Database access

```python
from p_plp.db import get_engine, get_engine_config, read_table

engine = get_engine(source_name="eunomia")
person_df = read_table(engine, "person", schema=get_engine_config(engine).cdm_schema)
```

## Local configuration

You can pass parameters directly to the library API.
For local development, environment variables in `.env` are also supported for database settings.
