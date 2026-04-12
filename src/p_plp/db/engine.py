from sqlalchemy import create_engine
from sqlalchemy.exc import NoSuchModuleError

from .config import get_source_config
from .validate import (
    validate_connection as _validate_connection,
    validate_schemas as _validate_schemas,
    validate_tables as _validate_tables,
)

DEFAULT_REQUIRED_CDM_TABLES = ["person", "observation_period", "cohort"]


def get_engine(
    source_name: str | None = None,
    *,
    database_url: str | None = None,
    database_path: str | None = None,
    cdm_schema: str | None = None,
    vocabulary_schema: str | None = None,
    work_schema: str | None = None,
    require_work_schema: bool = False,
    required_cdm_tables: list[str] | None = DEFAULT_REQUIRED_CDM_TABLES,
    required_work_tables: list[str] | None = None,
):
    """Create a configured SQLAlchemy engine for the requested datasource."""

    source_config = get_source_config(
        source_name=source_name,
        database_url=database_url,
        database_path=database_path,
        cdm_schema=cdm_schema,
        vocabulary_schema=vocabulary_schema,
        work_schema=work_schema,
    )
    try:
        engine = create_engine(source_config.database_url, future=True)
    except NoSuchModuleError as exc:
        if source_config.source_name == "eunomia":
            raise RuntimeError(
                "DuckDB SQLAlchemy support is not installed. Run `pip install duckdb-engine` or reinstall the project dependencies."
            ) from exc
        raise
    setattr(engine, "_plp_source_config", source_config)

    _validate_connection(engine)
    _validate_schemas(engine, require_work_schema=require_work_schema)
    if required_cdm_tables or required_work_tables:
        _validate_tables(
            engine,
            required_cdm_tables=required_cdm_tables,
            required_work_tables=required_work_tables,
        )

    return engine
