from sklearn.metrics import roc_auc_score, accuracy_score, classification_report
from p_plp.modeling.train import train

def evaluate():
    model, X_test, y_test = train()

    y_pred = model.predict(X_test)
    print("Accuracy:", round(accuracy_score(y_test, y_pred), 4))

    if hasattr(model, "predict_proba") and y_test.nunique() > 1:
        y_prob = model.predict_proba(X_test)[:, 1]
        print("ROC-AUC:", round(roc_auc_score(y_test, y_prob), 4))

    print()
    print(classification_report(y_test, y_pred, digits=3))


if __name__ == "__main__":
    evaluate()