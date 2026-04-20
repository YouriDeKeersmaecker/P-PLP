from __future__ import annotations

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV, StratifiedKFold, cross_validate
from sklearn.pipeline import Pipeline

from sklearn.feature_selection import SelectKBest, VarianceThreshold, f_classif, mutual_info_classif
from sklearn.svm import SVC

from .dataset import TARGET_COL, _prepare_X_y, split_dataset
from .preprocessing import build_preprocessor


def get_classifier(model_name: str = "logreg", **model_params):
    normalized = model_name.strip().lower()

    if normalized in {"logreg", "logistic", "logistic_regression"}:
        default_params = {
            "max_iter": 1000,
            "solver": "lbfgs",
            "penalty": "l2",
            "class_weight": "balanced",
            "random_state": 42,
        }
        default_params.update(model_params)
        return LogisticRegression(**default_params)

    if normalized in {"rf", "random_forest", "randomforest"}:
        default_params = {
            "n_estimators": 300,
            "random_state": 42,
            "n_jobs": -1,
        }
        default_params.update(model_params)
        return RandomForestClassifier(**default_params)

    if normalized in {"svm", "svc", "support_vector_machine"}:
        default_params = {
            "probability": True,
            "random_state": 42,
        }
        default_params.update(model_params)
        return SVC(**default_params)

    raise ValueError(
        "Unsupported model_name. Expected one of: "
        "logreg, logistic, logistic_regression, rf, random_forest, randomforest, "
        "svm, svc, support_vector_machine."
    )


def build_model_pipeline(
    X: pd.DataFrame,
    model_name: str = "logreg",
    model_params: dict | None = None,
    feature_selection_k: int = 20,
    feature_selection_score_func=mutual_info_classif,
) -> Pipeline:
    preprocessor = build_preprocessor(X)
    classifier = get_classifier(model_name=model_name, **(model_params or {}))
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("remove_constant", VarianceThreshold()),
            (
                "feature_selection",
                SelectKBest(
                    score_func=feature_selection_score_func,
                    k=feature_selection_k,
                ),
            ),
            ("classifier", classifier),
        ]
    )


def summarize_feature_selection(model: Pipeline, top_n: int | None = None) -> pd.DataFrame:
    preprocessor = model.named_steps["preprocessor"]
    feature_names = pd.Index(preprocessor.get_feature_names_out(), dtype="object")

    remove_constant = model.named_steps.get("remove_constant")
    if remove_constant is not None and hasattr(remove_constant, "get_support"):
        feature_names = feature_names[remove_constant.get_support()]

    selector = model.named_steps.get("feature_selection")
    if selector is None or not hasattr(selector, "get_support"):
        summary = pd.DataFrame({"feature": feature_names, "selected": True})
        return summary.head(top_n) if top_n is not None else summary

    selected_mask = selector.get_support()
    summary = pd.DataFrame(
        {
            "feature": feature_names,
            "score": getattr(selector, "scores_", None),
            "pvalue": getattr(selector, "pvalues_", None),
            "selected": selected_mask,
        }
    ).sort_values(["selected", "score"], ascending=[False, False], na_position="last")

    return summary.head(top_n) if top_n is not None else summary.reset_index(drop=True)


def cross_validate_pipeline(
    df: pd.DataFrame,
    model_name: str = "logreg",
    cv: int = 5,
    random_state: int = 42,
    scoring: str = "roc_auc",
    model_params: dict | None = None,
    feature_selection_k: int = 20,
    feature_selection_score_func=mutual_info_classif,
):
    X, y = _prepare_X_y(df, target_col=TARGET_COL)
    model = build_model_pipeline(
        X,
        model_name=model_name,
        model_params=model_params,
        feature_selection_k=feature_selection_k,
        feature_selection_score_func=feature_selection_score_func,
    )
    splitter = StratifiedKFold(n_splits=int(cv), shuffle=True, random_state=int(random_state))
    scores = cross_validate(
        model,
        X,
        y,
        cv=splitter,
        scoring=scoring,
        return_train_score=False,
    )

    test_scores = [float(score) for score in scores["test_score"]]
    return {
        "scoring": scoring,
        "fold_scores": test_scores,
        "mean_score": float(pd.Series(test_scores).mean()),
        "std_score": float(pd.Series(test_scores).std(ddof=0)),
    }


def grid_search_pipeline(
    df: pd.DataFrame,
    param_grid: dict,
    model_name: str = "logreg",
    cv: int = 5,
    random_state: int = 42,
    scoring: str = "roc_auc",
    n_jobs: int = -1,
    model_params: dict | None = None,
    feature_selection_k: int = 20,
    feature_selection_score_func=mutual_info_classif,
):
    X, y = _prepare_X_y(df, target_col=TARGET_COL)
    model = build_model_pipeline(
        X,
        model_name=model_name,
        model_params=model_params,
        feature_selection_k=feature_selection_k,
        feature_selection_score_func=feature_selection_score_func,
    )
    splitter = StratifiedKFold(n_splits=int(cv), shuffle=True, random_state=int(random_state))
    search = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        scoring=scoring,
        cv=splitter,
        n_jobs=int(n_jobs),
        refit=True,
    )
    search.fit(X, y)

    best_estimator = search.best_estimator_
    results_df = pd.DataFrame(search.cv_results_).sort_values("rank_test_score").reset_index(drop=True)
    return {
        "scoring": scoring,
        "best_params": dict(search.best_params_),
        "best_score": float(search.best_score_),
        "best_estimator": best_estimator,
        "feature_selection_summary": summarize_feature_selection(best_estimator),
        "results_df": results_df,
    }


def train_pipeline(
    df: pd.DataFrame,
    model_name: str = "logreg",
    test_size: float = 0.2,
    random_state: int = 42,
    model_params: dict | None = None,
    feature_selection_k: int = 20,
    feature_selection_score_func=mutual_info_classif,
):
    X_train, X_test, y_train, y_test = split_dataset(
        df,
        target_col=TARGET_COL,
        test_size=test_size,
        random_state=random_state,
    )

    model = build_model_pipeline(
        X_train,
        model_name=model_name,
        model_params=model_params,
        feature_selection_k=feature_selection_k,
        feature_selection_score_func=feature_selection_score_func,
    )
    model.fit(X_train, y_train)

    return model, X_test, y_test
