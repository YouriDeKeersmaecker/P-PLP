# P-PLP

## Project purpose

P-PLP is a bachelor project centered on the P-PLP tool for building and comparing patient-level prediction workflows on OMOP CDM data.
The core of the project is the Python tool in `src/p_plp/`.
The repository also contains separate R scripts that serve as examples for loading data and running PLP-related workflows around the tool.

The main goal is to support experimentation on PostgreSQL-backed Synthea data and DuckDB-backed Eunomia-style datasets through the P-PLP tool.

The Python package is structured around:

- `db/` for data-source configuration and database access
- `cohorts/` for exploratory cohort utilities, SQL-driven cohort loading, and time-at-risk generation
- `feature_engineering/` for feature creation
- `modeling/` for training, evaluation, and artifact export
- `config/` for declarative pipeline configuration
- `builders/` for turning specs into executable pipeline plans
- `pipeline.py` for the main library-facing orchestration API

## Setup

### Python

```powershell
python -m venv .venv
.\.venv\Scripts\Activate
pip install -e .[dev]
```

### R

R is only needed for the separate example scripts in `r/`.
Those scripts currently install missing dependencies themselves.

## Config files

Use local config files for machine-specific values and secrets.
Do not commit your real `.env` or `.Renviron`.

### Python config

Copy [`.env.example`](d:/School/BachelorProef/P-PLP/.env.example) to `.env` and replace the placeholders with your local values.

Typical variables:

```env
PLP_SOURCE=postgres
DATABASE_URL=postgresql+psycopg2://postgres:your-password-here@localhost:5432/synthealarge
EUNOMIA_DUCKDB_PATH=C:\path\to\eunomia.duckdb
```

### R config

Copy [`.Renviron.example`](d:/School/BachelorProef/P-PLP/.Renviron.example) to `.Renviron` and replace the placeholders with your local values.

Typical variables:

```env
PLP_DB_USER=postgres
PLP_DB_PASSWORD=your-password-here
PLP_JDBC_PATH=C:/jdbc
PLP_SYNTHEA_FILE_LOC=D:/path/to/synthea/output_xl/csv
PLP_VOCAB_FILE_LOC=D:/path/to/ETL_synthea
PLP_EUNOMIA_DUCKDB_PATH=D:/path/to/P-PLP/data/eunomia_synthea-allergies-10k.duckdb
```

The example R scripts in `r/` try to auto-load the repo's `.Renviron`, so they still work when R starts outside the project folder.

## Workflows

### Run the P-PLP tool

The main orchestration module is [`src/p_plp/pipeline.py`](d:/School/BachelorProef/P-PLP/src/p_plp/pipeline.py).
For backwards compatibility, [`src/p_plp/main.py`](d:/School/BachelorProef/P-PLP/src/p_plp/main.py) re-exports the same pipeline functions.

Overall pipeline flow with the main user choices:

```mermaid
flowchart TD
    A[Start: user wants to run P-PLP] --> B[Choose data source]

    B --> B1[PostgreSQL / OMOP database]
    B --> B2[DuckDB / Eunomia-style dataset]

    B1 --> C[Choose pipeline configuration]
    B2 --> C

    C --> C1[Use function-based API]
    C --> C2[Use spec-based API]

    C1 --> D[Provide target cohort SQL]
    C2 --> D
    D --> E[Provide outcome cohort SQL]
    E --> F[Provide risk window, lookback, model, output directory]
    F --> G[Build pipeline run config / pipeline plan]
    G --> H[Create database engine]

    H --> I[Generate target cohort]
    I --> J[Generate outcome cohort]
    J --> K[Generate labels and time-at-risk]

    K --> L[Choose feature generation]
    L --> L1[Demographics]
    L --> L2[Prior conditions]
    L --> L3[Prior drugs]
    L --> L4[Prior visits]
    L --> L5[future custom features]

    L1 --> M[Build final model dataset]
    L2 --> M
    L3 --> M
    L4 --> M
    L5 --> M

    M --> N[Load dataset into pandas]
    N --> O[Train model]
    O --> P[Evaluate model]
    P --> Q[Save artifacts]
    Q --> R[End]
```

Example usage from Python:

```python
from p_plp import run_postgres_pipeline

result = run_postgres_pipeline(
    target_cohort_sql="""
    drop table if exists plp_work.target_cohort;
    create table plp_work.target_cohort as
    select
        person_id as subject_id,
        current_date as cohort_start_date,
        current_date as cohort_end_date
    from cdm.person
    where person_id <= 100;
    """,
    outcome_cohort_sql="""
    drop table if exists plp_work.outcome_cohort;
    create table plp_work.outcome_cohort as
    select
        person_id as subject_id,
        current_date as cohort_start_date,
        current_date as cohort_end_date
    from cdm.person
    where person_id <= 100;
    """,
)
```

For DuckDB/Eunomia:

```python
from p_plp import run_eunomia_pipeline

result = run_eunomia_pipeline(
    database_path=r"C:\path\to\eunomia.duckdb",
    cdm_schema="main",
    work_schema="plp_work",
)
```

You can also build the pipeline declaratively:

```python
from p_plp import PipelineRunConfig, PredictionProblemConfig, run_pipeline_config

config = PipelineRunConfig(
    source_name="postgres",
    problem=PredictionProblemConfig(
        target_cohort_sql="""
        drop table if exists plp_work.target_cohort;
        create table plp_work.target_cohort as
        select
            person_id as subject_id,
            current_date as cohort_start_date,
            current_date as cohort_end_date
        from cdm.person
        where person_id <= 100;
        """,
        outcome_cohort_sql="""
        drop table if exists plp_work.outcome_cohort;
        create table plp_work.outcome_cohort as
        select
            person_id as subject_id,
            current_date as cohort_start_date,
            current_date as cohort_end_date
        from cdm.person
        where person_id <= 100;
        """,
    ),
)

result = run_pipeline_config(config)
```

## Extra: R examples and data preparation

These R scripts are separate examples. They are not the main P-PLP tool.
Use them when you want example OHDSI-based workflows or when you need to prepare data before using the main tool.

### Run the R PLP example

Use [`r/plp_example_script.R`](d:/School/BachelorProef/P-PLP/r/plp_example_script.R).

This script:
- connects to the OMOP database using `.Renviron`
- builds target and outcome cohorts
- extracts covariates with OHDSI packages
- runs a lasso logistic regression PLP model

Run it from R with:

```r
source("D:/School/BachelorProef/P-PLP/r/plp_example_script.R")
```

If `OhdsiShinyModules` is unavailable for your R version, the script skips `viewPlp()` and still finishes the run.

### Load Synthea data into OMOP on PostgreSQL

Use [`r/load_synthea_omop.R`](d:/School/BachelorProef/P-PLP/r/load_synthea_omop.R).

This script:
- reads PostgreSQL and path settings from `.Renviron`
- downloads JDBC drivers if needed
- creates the OMOP schemas
- loads Synthea source data and vocabulary through `ETLSyntheaBuilder`

Run it from R with:

```r
source("D:/School/BachelorProef/P-PLP/r/load_synthea_omop.R")
```

### Create a local Eunomia-style DuckDB dataset

Use [`r/load_enemea_omop.R`](d:/School/BachelorProef/P-PLP/r/load_enemea_omop.R).

This script:
- reads cache and DuckDB paths from `.Renviron`
- creates the local data directory when needed
- downloads or prepares the DuckDB-backed dataset with `eunomiaDir()`

Run it from R with:

```r
source("D:/School/BachelorProef/P-PLP/r/load_enemea_omop.R")
```

## Testing

Run the Python tests with:

```powershell
.\.venv\Scripts\Activate
pytest
```

Pytest uses `.pytest_tmp/` as a local temporary folder to avoid Windows temp-permission issues.

The separate R example scripts are currently set up for manual execution and validation rather than automated tests.
