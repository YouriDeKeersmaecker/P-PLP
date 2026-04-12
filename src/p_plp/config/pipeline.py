from dataclasses import dataclass, field


@dataclass(frozen=True)
class PredictionProblemConfig:
    """Configuration for Atlas-generated target and outcome cohorts."""

    target_cohort_sql: str = ""
    target_cohort_definition_id: int | None = None
    outcome_cohort_sql: str = ""
    outcome_cohort_definition_id: int | None = None


@dataclass(frozen=True)
class PipelineRunConfig:
    """Top-level configuration object describing one Atlas cohort loading run."""

    source_name: str | None = None
    database_url: str | None = None
    database_path: str | None = None
    cdm_schema: str | None = None
    problem: PredictionProblemConfig = field(default_factory=PredictionProblemConfig)
