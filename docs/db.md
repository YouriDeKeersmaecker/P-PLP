# Database Layer

`p_plp.db` contains the database helpers used by the rest of the package.

Main tasks:

- resolve connection settings
- create a SQLAlchemy engine
- validate schemas and required tables
- execute SQL and read tables into pandas

## Main functions

```python
from p_plp.db import (
    CdmTable,
    execute_sql,
    get_engine,
    get_engine_config,
    list_cdm_tables,
    read_sql_df,
    read_table,
    validate_connection,
    validate_schemas,
    validate_tables,
)
```

## Engine creation

```python
from p_plp.db import get_engine

engine = get_engine(
    source_name="postgres",
    database_url="postgresql+psycopg2://user:password@localhost:5432/db",
    cdm_schema="cdm",
    work_schema="plp_work",
)
```

```python
engine = get_engine(
    source_name="eunomia",
    database_path=r"C:\path\to\eunomia.duckdb",
    cdm_schema="main",
    work_schema="plp_work",
)
```

Supported source names:

- `postgres`
- `postgresql`
- `synthea`
- `duckdb`
- `eunomia`

## Reading data

```python
from p_plp.db import get_engine_config, read_table

person_df = read_table(engine, "person", schema=get_engine_config(engine).cdm_schema)
```

You can also use the `CdmTable` enum:

```python
from p_plp.db import CdmTable, get_engine_config, read_table

person_df = read_table(engine, CdmTable.PERSON, schema=get_engine_config(engine).cdm_schema)
```

## Running SQL

```python
from p_plp.db import execute_sql, read_sql_df

execute_sql(engine, "create schema if not exists plp_work")
df = read_sql_df(engine, "select * from cdm.person limit 5")
```

## Validation

`get_engine()` already validates the connection and tables. The validation helpers are also available directly when needed.

## API reference

::: p_plp.db

::: p_plp.db.config

::: p_plp.db.engine

::: p_plp.db.table_io

::: p_plp.db.validate

::: p_plp.db.sql_utils
