from dataclasses import dataclass

from .db import get_engine
from .cohorts import (
    load_atlas_cohort,
)
from .config import PipelineRunConfig, PredictionProblemConfig


@dataclass(frozen=True)
class PipelineRunPlan:
    """Resolved execution plan derived from an Atlas cohort loading spec."""

    config: PipelineRunConfig
    run_params: dict


def build_pipeline_plan(config: PipelineRunConfig) -> PipelineRunPlan:
    """Resolve an Atlas cohort loading config into concrete execution settings."""

    if not config.problem.target_cohort_sql.strip():
        raise ValueError("problem.target_cohort_sql is required.")
    if config.problem.target_cohort_definition_id is None:
        raise ValueError("problem.target_cohort_definition_id is required.")
    if not config.problem.outcome_cohort_sql.strip():
        raise ValueError("problem.outcome_cohort_sql is required.")
    if config.problem.outcome_cohort_definition_id is None:
        raise ValueError("problem.outcome_cohort_definition_id is required.")
    run_params = {
        "target_cohort_definition_id": config.problem.target_cohort_definition_id,
        "outcome_cohort_definition_id": config.problem.outcome_cohort_definition_id,
    }
    return PipelineRunPlan(
        config=config,
        run_params=run_params,
    )


def run_pipeline(
    source_name: str | None = None,
    database_url: str | None = None,
    database_path: str | None = None,
    cdm_schema: str | None = None,
    target_cohort_sql: str | None = None,
    target_cohort_definition_id: int | None = None,
    outcome_cohort_sql: str | None = None,
    outcome_cohort_definition_id: int | None = None,
):
    config = PipelineRunConfig(
        source_name=source_name,
        database_url=database_url,
        database_path=database_path,
        cdm_schema=cdm_schema,
        problem=PredictionProblemConfig(
            target_cohort_sql=target_cohort_sql or "",
            target_cohort_definition_id=target_cohort_definition_id,
            outcome_cohort_sql=outcome_cohort_sql or "",
            outcome_cohort_definition_id=outcome_cohort_definition_id,
        ),
    )
    return run_pipeline_config(config)


def run_pipeline_config(config: PipelineRunConfig):
    plan = build_pipeline_plan(config)
    engine = get_engine(
        source_name=config.source_name,
        database_url=config.database_url,
        database_path=config.database_path,
        cdm_schema=config.cdm_schema,
    )

    target_cohort = load_atlas_cohort(
        engine,
        sql=plan.config.problem.target_cohort_sql,
        cohort_definition_id=plan.config.problem.target_cohort_definition_id,
    )
    outcome_cohort = load_atlas_cohort(
        engine,
        sql=plan.config.problem.outcome_cohort_sql,
        cohort_definition_id=plan.config.problem.outcome_cohort_definition_id,
    )

    return {
        "engine": engine,
        "target_cohort": target_cohort,
        "outcome_cohort": outcome_cohort,
        "plan": plan,
    }


def run_pipeline_spec(config: PipelineRunConfig):
    """Backward-compatible alias for the older spec-based pipeline entry point."""

    return run_pipeline_config(config)


def run_postgres_pipeline(**kwargs):
    return run_pipeline(source_name="postgres", **kwargs)


def run_outpatient_revisit_pipeline(**kwargs):
    return run_pipeline(**kwargs)


def run_condition_to_condition_pipeline(**kwargs):
    return run_pipeline(**kwargs)
