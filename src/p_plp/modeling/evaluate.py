from __future__ import annotations

from math import sqrt

from sklearn.metrics import (
    accuracy_score,
    classification_report,
    recall_score,
    roc_auc_score,
)


def _bounded_wald_ci(
    estimate: float | None,
    n: int,
    z: float = 1.96,
) -> tuple[float, float] | None:
    """Return a simple 95% Wald CI clipped to [0, 1]."""

    if estimate is None or n <= 0:
        return None

    margin = z * sqrt((estimate * (1 - estimate)) / n)
    lower = max(0.0, float(estimate - margin))
    upper = min(1.0, float(estimate + margin))
    return (lower, upper)


def _print_metric(name: str, value: float | None, ci: tuple[float, float] | None = None) -> None:
    if value is None:
        return

    print(f"{name}:", round(value, 4))
    if ci is not None:
        print(f"{name} 95% CI:", f"({ci[0]:.4f}, {ci[1]:.4f})")


def evaluate_cross_validation(cv_results):
    print("Scoring:", cv_results["scoring"])
    print("Fold scores:", [round(score, 4) for score in cv_results["fold_scores"]])
    print("Mean score:", round(cv_results["mean_score"], 4))
    print("Std score:", round(cv_results["std_score"], 4))


def evaluate_grid_search(grid_search_results):
    print("Scoring:", grid_search_results["scoring"])
    print("Best score:", round(grid_search_results["best_score"], 4))
    print("Best params:", grid_search_results["best_params"])


def evaluate(model, X_test, y_test):
    y_pred = model.predict(X_test)

    total = int(len(y_test))
    positives = int((y_test == 1).sum())
    negatives = int((y_test == 0).sum())

    accuracy = float(accuracy_score(y_test, y_pred))
    sensitivity = (
        float(recall_score(y_test, y_pred, pos_label=1, zero_division=0))
        if positives
        else None
    )
    specificity = (
        float(recall_score(y_test, y_pred, pos_label=0, zero_division=0))
        if negatives
        else None
    )

    metrics = {
        "accuracy": accuracy,
        "accuracy_ci": _bounded_wald_ci(accuracy, total),
        "sensitivity": sensitivity,
        "sensitivity_ci": _bounded_wald_ci(sensitivity, positives),
        "specificity": specificity,
        "specificity_ci": _bounded_wald_ci(specificity, negatives),
        "roc_auc": None,
        "roc_auc_ci": None,
        "prevalence": float(y_test.mean()) if len(y_test) else None,
    }

    _print_metric("Accuracy", metrics["accuracy"], metrics["accuracy_ci"])
    _print_metric("Sensitivity", metrics["sensitivity"], metrics["sensitivity_ci"])
    _print_metric("Specificity", metrics["specificity"], metrics["specificity_ci"])

    if hasattr(model, "predict_proba") and y_test.nunique() > 1:
        y_prob = model.predict_proba(X_test)[:, 1]
        y_pred = (y_prob >= 0.2).astype(int)
        metrics["roc_auc"] = float(roc_auc_score(y_test, y_prob))
        metrics["roc_auc_ci"] = _bounded_wald_ci(metrics["roc_auc"], total)
        _print_metric("ROC-AUC", metrics["roc_auc"], metrics["roc_auc_ci"])

    print("Prevalence:", round(metrics["prevalence"], 4) if metrics["prevalence"] is not None else "NA")
    print()
    print(classification_report(y_test, y_pred, digits=3, zero_division=0))

    return metrics
