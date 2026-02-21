from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression

from p_plp.modeling import *
from p_plp.db import *

def train_pipeline(df, test_size=0.2, random_state=42):
    X, y, num_cols, cat_cols = make_X_y(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=float(test_size),
        random_state=int(random_state),
        stratify=y if y.nunique() > 1 else None,
    )

    transformers = []

    if num_cols:
        transformers.append(
            (
                "num",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                num_cols,
            )
        )

    if cat_cols:
        transformers.append(
            (
                "cat",
                Pipeline(
                    [
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                cat_cols,
            )
        )

    preprocess = ColumnTransformer(transformers, remainder="drop")

    model = Pipeline(
        [
            ("preprocess", preprocess),
            ("clf", LogisticRegression(max_iter=2000)),
        ]
    )

    model.fit(X_train, y_train)
    return model, X_test, y_test