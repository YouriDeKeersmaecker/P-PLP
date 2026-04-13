from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

from p_plp.modeling.dataset import split_dataset


def get_classifier(model_name: str = "logreg", random_state: int = 42):
    model_key = model_name.lower()

    registry = {
        "logreg": LogisticRegression(max_iter=2000, random_state=int(random_state)),
        "random_forest": RandomForestClassifier(
            n_estimators=200,
            random_state=int(random_state),
        ),
        "gradient_boosting": GradientBoostingClassifier(
            random_state=int(random_state),
        ),
    }

    if model_key not in registry:
        available = ", ".join(sorted(registry))
        raise ValueError(f"Unknown model_name '{model_name}'. Available models: {available}")

    return registry[model_key]


def train_pipeline(df, clf=None, model_name: str = "logreg", test_size=0.2, random_state=42):
    X_train, X_test, y_train, y_test = split_dataset(
        df,
        test_size=test_size,
        random_state=random_state,
    )

    num_cols = X_train.select_dtypes(include=["number", "bool"]).columns.tolist()
    cat_cols = [c for c in X_train.columns if c not in num_cols]

    transformers = []

    if num_cols:
        transformers.append(
            ("num",
             Pipeline([
                 ("imputer", SimpleImputer(strategy="median")),
                 ("scaler", StandardScaler()),
             ]),
             num_cols)
        )

    if cat_cols:
        transformers.append(
            ("cat",
             Pipeline([
                 ("imputer", SimpleImputer(strategy="most_frequent")),
                 ("onehot", OneHotEncoder(handle_unknown="ignore")),
             ]),
             cat_cols)
        )

    preprocess = ColumnTransformer(transformers, remainder="drop")

    if clf is None:
        clf = get_classifier(model_name=model_name, random_state=random_state)
    else:
        clf = clone(clf)

    model = Pipeline([
        ("preprocess", preprocess),
        ("clf", clf),
    ])

    model.fit(X_train, y_train)
    return model, X_test, y_test
