import pytest

from p_plp.db.config import get_engine_config, get_source_config


def test_get_source_config_for_postgres_explicit_values():
    config = get_source_config(
        source_name="postgres",
        database_url="postgresql+psycopg2://user:pw@localhost:5432/db",
        cdm_schema="cdm_test",
        work_schema="work_test",
    )

    assert config.source_name == "postgres"
    assert config.database_url == "postgresql+psycopg2://user:pw@localhost:5432/db"
    assert config.cdm_schema == "cdm_test"
    assert config.vocabulary_schema == "cdm_test"
    assert config.work_schema == "work_test"
    assert config.database_path is None


def test_get_source_config_for_eunomia_resolves_memory_path():
    config = get_source_config(
        source_name="eunomia",
        database_path=":memory:",
        cdm_schema="main",
        work_schema="plp_work",
    )

    assert config.source_name == "eunomia"
    assert config.database_url == "duckdb:///:memory:"
    assert config.database_path == ":memory:"
    assert config.cdm_schema == "main"
    assert config.vocabulary_schema == "main"
    assert config.work_schema == "plp_work"


def test_get_source_config_accepts_explicit_vocabulary_schema():
    config = get_source_config(
        source_name="postgres",
        database_url="postgresql+psycopg2://user:pw@localhost:5432/db",
        cdm_schema="cdm_test",
        vocabulary_schema="vocab_test",
    )

    assert config.vocabulary_schema == "vocab_test"


def test_get_source_config_rejects_invalid_source_name():
    with pytest.raises(ValueError, match="Unsupported source_name"):
        get_source_config(
            source_name="sqlite",
            database_url="sqlite:///tmp.db",
        )


def test_get_engine_config_returns_none_when_not_attached():
    class DummyEngine:
        pass

    assert get_engine_config(DummyEngine()) is None


def test_get_source_config_requires_explicit_source_name():
    with pytest.raises(ValueError, match="source_name is required"):
        get_source_config(
            database_url="postgresql+psycopg2://user:pw@localhost:5432/db",
            cdm_schema="cdm_test",
        )


def test_get_source_config_requires_explicit_cdm_schema():
    with pytest.raises(ValueError, match="cdm_schema is required"):
        get_source_config(
            source_name="postgres",
            database_url="postgresql+psycopg2://user:pw@localhost:5432/db",
        )
