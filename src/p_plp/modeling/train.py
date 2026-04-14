from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV, StratifiedKFold, cross_validate
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.feature_selection import SelectKBest, f_classif

from .dataset import TARGET_COL, split_dataset

DEFAULT_TARGET_CANDIDATES = (TARGET_COL, "outcome_flag")


@dataclass(frozen=True)
class FeatureGroups:
    binary_numeric: list[str]
    continuous_numeric: list[str]
    categorical: list[str]


def _resolve_target_col(df: pd.DataFrame, target_col: str | None = None) -> str:
    if target_col is not None:
        if target_col not in df.columns:
            raise ValueError(f"Missing target column: {target_col}")
        return target_col

    for candidate in DEFAULT_TARGET_CANDIDATES:
        if candidate in df.columns:
            return candidate

    raise ValueError(
        "Could not infer target column. Pass target_col explicitly or include one of: "
        f"{', '.join(DEFAULT_TARGET_CANDIDATES)}."
    )


def _is_binary_numeric(series: pd.Series) -> bool:
    if not pd.api.types.is_numeric_dtype(series):
        return False
    non_null_values = pd.Series(series.dropna().unique())
    if non_null_values.empty:
        return True
    return non_null_values.isin([0, 1]).all()


def infer_feature_groups(X: pd.DataFrame) -> FeatureGroups:
    binary_numeric: list[str] = []
    continuous_numeric: list[str] = []
    categorical: list[str] = []

    for column in X.columns:
        series = X[column]
        if pd.api.types.is_numeric_dtype(series):
            if _is_binary_numeric(series):
                binary_numeric.append(column)
            else:
                continuous_numeric.append(column)
        else:
            categorical.append(column)

    return FeatureGroups(
        binary_numeric=binary_numeric,
        continuous_numeric=continuous_numeric,
        categorical=categorical,
    )


def build_preprocessor(X: pd.DataFrame) -> ColumnTransformer:
    groups = infer_feature_groups(X)
    transformers: list[tuple[str, Pipeline | str, list[str]]] = []

    if groups.binary_numeric:
        transformers.append(
            (
                "binary",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                    ]
                ),
                groups.binary_numeric,
            )
        )

    if groups.continuous_numeric:
        transformers.append(
            (
                "continuous",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                groups.continuous_numeric,
            )
        )

    if groups.categorical:
        transformers.append(
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        (
                            "encoder",
                            OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                        ),
                    ]
                ),
                groups.categorical,
            )
        )

    return ColumnTransformer(transformers=transformers, remainder="drop")


def get_classifier(model_name: str = "logreg", **model_params):
    normalized = model_name.strip().lower()

    if normalized in {"logreg", "logistic", "logistic_regression"}:
        default_params = {
            "max_iter": 1000,
            "solver": "liblinear",
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

    raise ValueError(
        "Unsupported model_name. Expected one of: "
        "logreg, logistic, logistic_regression, rf, random_forest, randomforest."
    )


def build_model_pipeline(
    X: pd.DataFrame,
    model_name: str = "logreg",
    model_params: dict | None = None,
) -> Pipeline:
    preprocessor = build_preprocessor(X)
    classifier = get_classifier(model_name=model_name, **(model_params or {}))
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("feature_selection", SelectKBest(score_func=f_classif, k="all")),
            ("classifier", classifier),
        ]
    )


def _prepare_model_data(
    df: pd.DataFrame,
    target_col: str | None = None,
) -> tuple[pd.DataFrame, pd.Series, str]:
    resolved_target_col = _resolve_target_col(df, target_col=target_col)
    X = df.drop(columns=[resolved_target_col]).copy().dropna(axis=1, how="all")
    y = df[resolved_target_col].astype(int)
    return X, y, resolved_target_col


def cross_validate_pipeline(
    df: pd.DataFrame,
    model_name: str = "logreg",
    target_col: str | None = None,
    cv: int = 5,
    random_state: int = 42,
    scoring: str = "roc_auc",
    model_params: dict | None = None,
):
    X, y, _ = _prepare_model_data(df, target_col=target_col)
    model = build_model_pipeline(X, model_name=model_name, model_params=model_params)
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
    target_col: str | None = None,
    cv: int = 5,
    random_state: int = 42,
    scoring: str = "roc_auc",
    n_jobs: int = 1,
    model_params: dict | None = None,
):
    X, y, _ = _prepare_model_data(df, target_col=target_col)
    model = build_model_pipeline(X, model_name=model_name, model_params=model_params)
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
        "results_df": results_df,
    }


def train_pipeline(
    df: pd.DataFrame,
    model_name: str = "logreg",
    target_col: str | None = None,
    test_size: float = 0.2,
    random_state: int = 42,
    model_params: dict | None = None,
):
    _, _, resolved_target_col = _prepare_model_data(df, target_col=target_col)
    X_train, X_test, y_train, y_test = split_dataset(
        df,
        target_col=resolved_target_col,
        test_size=test_size,
        random_state=random_state,
    )

    model = build_model_pipeline(X_train, model_name=model_name, model_params=model_params)
    model.fit(X_train, y_train)

    return model, X_test, y_test
