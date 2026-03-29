from sklearn.base import clone
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier

from p_plp.modeling.dataset import make_X_y


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
