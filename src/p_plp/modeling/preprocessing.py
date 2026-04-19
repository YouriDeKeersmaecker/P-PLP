from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


@dataclass(frozen=True)
class FeatureGroups:
    binary_numeric: list[str]
    continuous_numeric: list[str]
    categorical: list[str]


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


def _is_binary_numeric(series: pd.Series) -> bool:
    if not pd.api.types.is_numeric_dtype(series):
        return False
    non_null_values = pd.Series(series.dropna().unique())
    if non_null_values.empty:
        return True
    return non_null_values.isin([0, 1]).all()


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
