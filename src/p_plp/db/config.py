from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SourceConfig:
    """Resolved datasource configuration used by the database layer."""

    source_name: str
    database_url: str
    cdm_schema: str
    vocabulary_schema: str
    work_schema: str | None = None
    database_path: str | None = None

def _normalize_source_name(source_name: str | None) -> str:
    if not source_name:
        raise ValueError("source_name is required.")
    candidate = source_name.strip().lower()
    aliases = {
        "postgres": "postgres",
        "postgresql": "postgres",
        "synthea": "postgres",
        "duckdb": "eunomia",
        "eunomia": "eunomia",
    }
    if candidate not in aliases:
        raise ValueError(
            "Unsupported source_name. Expected one of: postgres, postgresql, synthea, duckdb, eunomia."
        )
    return aliases[candidate]


def _resolve_database_path(database_path: str | None) -> str:
    resolved = database_path
    if not resolved:
        raise ValueError("database_path is required for eunomia.")
    if resolved == ":memory:":
        return resolved
    return str(Path(resolved).expanduser().resolve())


def get_source_config(
    source_name: str | None = None,
    database_url: str | None = None,
    database_path: str | None = None,
    cdm_schema: str | None = None,
    vocabulary_schema: str | None = None,
    work_schema: str | None = None,
) -> SourceConfig:
    """Resolve datasource settings for a PostgreSQL or Eunomia/DuckDB connection."""

    normalized_source = _normalize_source_name(source_name)

    if not cdm_schema:
        raise ValueError("cdm_schema is required.")

    if normalized_source == "postgres":
        resolved_database_url = database_url
        if not resolved_database_url:
            raise ValueError("database_url is required for postgres.")
        return SourceConfig(
            source_name="postgres",
            database_url=resolved_database_url,
            cdm_schema=cdm_schema,
            vocabulary_schema=vocabulary_schema or cdm_schema,
            work_schema=work_schema,
        )

    resolved_database_path = _resolve_database_path(database_path)
    resolved_database_url = database_url or f"duckdb:///{resolved_database_path}"
    return SourceConfig(
        source_name="eunomia",
        database_url=resolved_database_url,
        cdm_schema=cdm_schema,
        vocabulary_schema=vocabulary_schema or cdm_schema,
        work_schema=work_schema,
        database_path=resolved_database_path,
    )


def get_engine_config(engine) -> SourceConfig | None:
    """Return the resolved source configuration attached to an engine, if available."""

    return getattr(engine, "_plp_source_config", None)
