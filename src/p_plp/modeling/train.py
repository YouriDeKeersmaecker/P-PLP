from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

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


def train_pipeline(
    df: pd.DataFrame,
    model_name: str = "logreg",
    target_col: str | None = None,
    test_size: float = 0.2,
    random_state: int = 42,
    model_params: dict | None = None,
):
    resolved_target_col = _resolve_target_col(df, target_col=target_col)
    X_train, X_test, y_train, y_test = split_dataset(
        df,
        target_col=resolved_target_col,
        test_size=test_size,
        random_state=random_state,
    )

    preprocessor = build_preprocessor(X_train)
    classifier = get_classifier(model_name=model_name, **(model_params or {}))

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", classifier),
        ]
    )
    model.fit(X_train, y_train)

    return model, X_test, y_test
