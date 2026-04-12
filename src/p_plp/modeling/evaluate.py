from sklearn.metrics import accuracy_score, classification_report, roc_auc_score


def evaluate(model, X_test, y_test):
    y_pred = model.predict(X_test)
    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "roc_auc": None,
        "prevalence": float(y_test.mean()) if len(y_test) else None,
    }

    print("Accuracy:", round(metrics["accuracy"], 4))

    if hasattr(model, "predict_proba") and y_test.nunique() > 1:
        y_prob = model.predict_proba(X_test)[:, 1]
        metrics["roc_auc"] = float(roc_auc_score(y_test, y_prob))
        print("ROC-AUC:", round(metrics["roc_auc"], 4))

    print("Prevalence:", round(metrics["prevalence"], 4) if metrics["prevalence"] is not None else "NA")
    print()
    print(classification_report(y_test, y_pred, digits=3, zero_division=0))

    return metrics

