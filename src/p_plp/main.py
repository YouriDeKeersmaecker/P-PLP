from .cohorts import *
from .db import *
from .feature_engineering import *
from .modeling import *


def run_pipeline(
    target_concept_id: int = 40481087,
    outcome_concept_id: int = 133228,
    risk_start_days: int = 1,
    risk_end_days: int = 365,
    lookback_days: int = 365,
    model_name: str = "logreg",
):
    engine = get_engine()

    generate_target_cohort(engine, target_concept_id)
    generate_outcome_cohort(engine, outcome_concept_id)
    generate_labels_time_at_risk(
        engine,
        risk_start_days=risk_start_days,
        risk_end_days=risk_end_days,
    )

    build_demographic_features(engine)
    build_prior_condition_count_features(engine, lookback_days=lookback_days)
    build_prior_drug_count_features(engine, lookback_days=lookback_days)
    build_prior_visit_count_features(engine, lookback_days=lookback_days)
    build_model_dataset(engine)

    df_model = get_model_dataset(engine)
    model, X_test, y_test = train_pipeline(df_model, model_name=model_name)
    evaluate(model, X_test, y_test)

    return {
        "engine": engine,
        "df_model": df_model,
        "model": model,
        "X_test": X_test,
        "y_test": y_test,
    }

if __name__ == "__main__":
    run_pipeline()
