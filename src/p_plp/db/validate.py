from sqlalchemy import text

from .config import get_engine_config


def validate_connection(engine) -> bool:
    """Check that the database is reachable."""

    with engine.connect() as conn:
        result = conn.execute(text("select 1"))
        if result.scalar() != 1:
            raise ValueError("Database connection check failed.")
    return True


def validate_schemas(engine, require_work_schema: bool = False) -> bool:
    """Check that the configured schemas exist."""

    config = get_engine_config(engine)
    if config is None:
        raise ValueError("No engine configuration is attached.")

    with engine.connect() as conn:
        schemas = {
            row[0]
            for row in conn.execute(
                text("select schema_name from information_schema.schemata")
            )
        }

    if config.cdm_schema not in schemas:
        raise ValueError(f"Configured CDM schema does not exist: {config.cdm_schema}")

    if config.vocabulary_schema not in schemas:
        raise ValueError(
            f"Configured vocabulary schema does not exist: {config.vocabulary_schema}"
        )

    if require_work_schema:
        if not config.work_schema:
            raise ValueError("A work schema is required but none is configured.")
        if config.work_schema not in schemas:
            raise ValueError(f"Configured work schema does not exist: {config.work_schema}")

    return True


def validate_tables(
    engine,
    required_cdm_tables: list[str] | None = None,
    required_work_tables: list[str] | None = None,
) -> bool:
    """Check that required tables exist in the configured schemas."""

    config = get_engine_config(engine)
    if config is None:
        raise ValueError("No engine configuration is attached.")

    required_cdm_tables = required_cdm_tables or []
    required_work_tables = required_work_tables or []

    if required_cdm_tables:
        cdm_tables = _get_table_names(engine, config.cdm_schema)
        missing_cdm_tables = sorted(set(required_cdm_tables) - cdm_tables)
        if missing_cdm_tables:
            raise ValueError(
                f"Missing required CDM tables in schema '{config.cdm_schema}': {', '.join(missing_cdm_tables)}"
            )

    if required_work_tables:
        if not config.work_schema:
            raise ValueError("Work tables were requested but no work schema is configured.")
        work_tables = _get_table_names(engine, config.work_schema)
        missing_work_tables = sorted(set(required_work_tables) - work_tables)
        if missing_work_tables:
            raise ValueError(
                f"Missing required work tables in schema '{config.work_schema}': {', '.join(missing_work_tables)}"
            )

    return True


def _get_table_names(engine, schema: str) -> set[str]:
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                select table_name
                from information_schema.tables
                where table_schema = :schema
                  and table_type = 'BASE TABLE'
                """
            ),
            {"schema": schema},
        )
        return {row[0] for row in result}
