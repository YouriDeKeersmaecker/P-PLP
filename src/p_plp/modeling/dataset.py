import pandas as pd
from sklearn.model_selection import train_test_split

TARGET_COL = "label"

def _prepare_X_y(
    df: pd.DataFrame,
    target_col: str = TARGET_COL,
):
    if target_col not in df.columns:
        raise ValueError(f"Missing target column: {target_col}")

    y = df[target_col].astype(int)
    feature_cols = [col for col in df.columns if col != target_col]
    X = df[feature_cols].copy().dropna(axis=1, how="all")

    return X, y


def split_dataset(
    df: pd.DataFrame,
    target_col: str = TARGET_COL,
    test_size: float = 0.2,
    random_state: int = 42,
):
    X, y = _prepare_X_y(df, target_col=target_col)

    return train_test_split(
        X,
        y,
        test_size=float(test_size),
        random_state=int(random_state),
        stratify=y if y.nunique() > 1 else None,
    )
