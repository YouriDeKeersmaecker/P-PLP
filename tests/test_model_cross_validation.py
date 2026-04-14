import pandas as pd

from p_plp.modeling.train import cross_validate_pipeline


def test_cross_validate_pipeline_returns_five_fold_summary():
    df = pd.DataFrame(
        {
            "subject_id": range(1, 21),
            "age": [30, 32, 34, 36, 38, 40, 42, 44, 46, 48] * 2,
            "gender_concept_id": [8507, 8532] * 10,
            "n_prior_conditions": [0, 1, 1, 2, 2, 3, 3, 4, 4, 5] * 2,
            "label": [0, 0, 0, 0, 1, 1, 1, 1, 1, 0] * 2,
        }
    )

    result = cross_validate_pipeline(
        df,
        model_name="logreg",
        target_col="label",
        cv=5,
        random_state=42,
        scoring="roc_auc",
    )

    assert result["scoring"] == "roc_auc"
    assert len(result["fold_scores"]) == 5
    assert all(isinstance(score, float) for score in result["fold_scores"])
    assert isinstance(result["mean_score"], float)
    assert isinstance(result["std_score"], float)
