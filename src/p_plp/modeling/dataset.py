import pandas as pd
from p_plp.feature_engineering import *

TARGET_COL = "outcome_flag"
DROP_COLS = {
    "subject_id",
    "cohort_start_date",
    "cohort_end_date",
    "index_date",
}

def make_X_y(df: pd.DataFrame):
    if TARGET_COL not in df.columns:
        raise ValueError(f"Missing target column: {TARGET_COL}")

    y = df[TARGET_COL].astype(int)

    # Dynamic features: everything except ids/dates/target
    feature_cols = [c for c in df.columns if c != TARGET_COL and c not in DROP_COLS]
    X = df[feature_cols].copy()

    X = X.dropna(axis=1, how="all")

    # Infer column types
    num_cols = X.select_dtypes(include=["number", "bool"]).columns.tolist()
    cat_cols = [c for c in X.columns if c not in num_cols]

    return X, y, num_cols, cat_cols