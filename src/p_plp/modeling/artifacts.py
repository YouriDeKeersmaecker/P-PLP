import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, roc_auc_score


def _to_serializable(value):
    if hasattr(value, "item"):
        return value.item()
    return value


def _get_feature_summary(model) -> pd.DataFrame:
    preprocess = model.named_steps["preprocess"]
    clf = model.named_steps["clf"]

    feature_names = preprocess.get_feature_names_out()
    data = {"feature_name": feature_names}

    if hasattr(clf, "coef_"):
        coefs = clf.coef_[0] if getattr(clf.coef_, "ndim", 1) > 1 else clf.coef_
        data["coefficient"] = coefs

    if hasattr(clf, "feature_importances_"):
        data["feature_importance"] = clf.feature_importances_

    return pd.DataFrame(data)


def save_model_outputs(model, X_test, y_test, output_dir, run_params: dict | None = None) -> dict:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else None

    prevalence = float(y_test.mean()) if len(y_test) else None
    accuracy = float(accuracy_score(y_test, y_pred)) if len(y_test) else None
    roc_auc = None
    if y_prob is not None and getattr(y_test, "nunique", lambda: 0)() > 1:
        roc_auc = float(roc_auc_score(y_test, y_prob))

    labels = sorted(y_test.astype(int).unique().tolist()) if len(y_test) else [0, 1]
    cm = confusion_matrix(y_test, y_pred, labels=labels)
    cm_df = pd.DataFrame(cm, index=labels, columns=labels)
    cm_df.index.name = "actual"
    cm_df.columns.name = "predicted"
    cm_df.to_csv(output_path / "confusion_matrix.csv")

    feature_summary = _get_feature_summary(model)
    feature_summary.to_csv(output_path / "feature_coefficients.csv", index=False)

    report = classification_report(y_test, y_pred, digits=3, output_dict=True, zero_division=0)
    metrics = {
        "accuracy": accuracy,
        "roc_auc": roc_auc,
        "prevalence": prevalence,
        "n_test_rows": int(len(y_test)),
        "positive_outcomes": int(y_test.sum()),
        "confusion_matrix_labels": labels,
        "confusion_matrix": cm.tolist(),
        "classification_report": {
            key: {metric: _to_serializable(val) for metric, val in value.items()}
            if isinstance(value, dict)
            else _to_serializable(value)
            for key, value in report.items()
        },
    }

    with (output_path / "metrics.json").open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_class": type(model.named_steps["clf"]).__name__,
        "run_params": run_params or {},
        "artifacts": {
            "metrics": "metrics.json",
            "confusion_matrix": "confusion_matrix.csv",
            "feature_coefficients": "feature_coefficients.csv",
        },
    }

    with (output_path / "run_manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return {
        "output_dir": str(output_path),
        "metrics": metrics,
        "manifest": manifest,
        "feature_summary": feature_summary,
        "confusion_matrix": cm_df,
    }
