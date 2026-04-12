import json

import pandas as pd

from p_plp.modeling import save_model_outputs, train_pipeline


def test_save_model_outputs_writes_expected_artifacts(tmp_path):
    df = pd.DataFrame(
        {
            "subject_id": range(1, 11),
            "index_date": pd.date_range("2025-01-01", periods=10, freq="D"),
            "age": [30, 32, 34, 36, 38, 40, 42, 44, 46, 48],
            "gender_concept_id": [8507, 8532] * 5,
            "n_prior_conditions": [0, 1, 1, 2, 2, 3, 3, 4, 4, 5],
            "n_prior_drug_exposures": [0, 0, 1, 1, 2, 2, 3, 3, 4, 4],
            "n_prior_visits": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
            "outcome_flag": [0, 0, 0, 0, 1, 1, 1, 1, 1, 0],
        }
    )

    model, X_test, y_test = train_pipeline(df, model_name="logreg", test_size=0.3, random_state=7)
    result = save_model_outputs(
        model,
        X_test,
        y_test,
        tmp_path,
        run_params={"model_name": "logreg", "lookback_days": 365},
    )

    assert (tmp_path / "metrics.json").exists()
    assert (tmp_path / "confusion_matrix.csv").exists()
    assert (tmp_path / "feature_coefficients.csv").exists()
    assert (tmp_path / "run_manifest.json").exists()

    metrics = json.loads((tmp_path / "metrics.json").read_text(encoding="utf-8"))
    manifest = json.loads((tmp_path / "run_manifest.json").read_text(encoding="utf-8"))
    feature_summary = pd.read_csv(tmp_path / "feature_coefficients.csv")

    assert "accuracy" in metrics
    assert "prevalence" in metrics
    assert "roc_auc" in metrics
    assert manifest["run_params"]["model_name"] == "logreg"
    assert "feature_name" in feature_summary.columns
    assert "coefficient" in feature_summary.columns
    assert result["output_dir"] == str(tmp_path)
