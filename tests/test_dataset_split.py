import pandas as pd

from p_plp.modeling.dataset import split_dataset


def test_split_dataset_returns_expected_shapes_and_target_balance():
    df = pd.DataFrame(
        {
            "subject_id": range(1, 11),
            "index_date": pd.date_range("2025-01-01", periods=10, freq="D"),
            "age": [30, 32, 34, 36, 38, 40, 42, 44, 46, 48],
            "gender_concept_id": [8507, 8532] * 5,
            "n_prior_conditions": [0, 1, 1, 2, 2, 3, 3, 4, 4, 5],
            "label": [0, 0, 0, 0, 1, 1, 1, 1, 1, 0],
        }
    )

    X_train, X_test, y_train, y_test = split_dataset(df, test_size=0.2, random_state=42)

    assert len(X_train) == 8
    assert len(X_test) == 2
    assert len(y_train) == 8
    assert len(y_test) == 2
    assert "label" not in X_train.columns
    assert "subject_id" in X_train.columns
    assert "index_date" in X_train.columns
    assert y_train.sum() == 4
    assert y_test.sum() == 1
