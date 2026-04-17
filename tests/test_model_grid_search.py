import pandas as pd

from p_plp.modeling.train import grid_search_pipeline


def test_grid_search_pipeline_returns_best_params_and_score():
    df = pd.DataFrame(
        {
            "subject_id": range(1, 21),
            "age": [30, 32, 34, 36, 38, 40, 42, 44, 46, 48] * 2,
            "gender_concept_id": [8507, 8532] * 10,
            "n_prior_conditions": [0, 1, 1, 2, 2, 3, 3, 4, 4, 5] * 2,
            "outcome_flag": [0, 0, 0, 0, 1, 1, 1, 1, 1, 0] * 2,
        }
    )

    result = grid_search_pipeline(
        df,
        model_name="logreg",
        cv=5,
        random_state=42,
        scoring="roc_auc",
        n_jobs=1,
        param_grid={
            "classifier__C": [0.1, 1.0],
        },
    )

    assert result["scoring"] == "roc_auc"
    assert result["best_params"]["classifier__C"] in {0.1, 1.0}
    assert isinstance(result["best_score"], float)
    assert result["best_estimator"] is not None
    assert not result["results_df"].empty


def test_grid_search_pipeline_supports_feature_selection_step():
    df = pd.DataFrame(
        {
            "subject_id": range(1, 21),
            "age": [30, 32, 34, 36, 38, 40, 42, 44, 46, 48] * 2,
            "gender_concept_id": [8507, 8532] * 10,
            "n_prior_conditions": [0, 1, 1, 2, 2, 3, 3, 4, 4, 5] * 2,
            "outcome_flag": [0, 0, 0, 0, 1, 1, 1, 1, 1, 0] * 2,
        }
    )

    result = grid_search_pipeline(
        df,
        model_name="logreg",
        cv=5,
        random_state=42,
        scoring="roc_auc",
        n_jobs=1,
        param_grid={
            "feature_selection__k": [1, "all"],
            "classifier__C": [0.1, 1.0],
        },
    )

    assert result["best_params"]["feature_selection__k"] in {1, "all"}
    assert "param_feature_selection__k" in result["results_df"].columns
    assert not result["feature_selection_summary"].empty
    assert "feature" in result["feature_selection_summary"].columns
    assert "selected" in result["feature_selection_summary"].columns
