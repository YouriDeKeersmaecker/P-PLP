from __future__ import annotations

import pandas as pd


def plot_logreg_feature_importance(model, top_n: int = 20):
    import matplotlib.pyplot as plt

    preprocessor = model.named_steps["preprocessor"]
    feature_names = pd.Index(preprocessor.get_feature_names_out(), dtype="object")

    remove_constant = model.named_steps["remove_constant"]
    feature_names = feature_names[remove_constant.get_support()]

    selector = model.named_steps["feature_selection"]
    feature_names = feature_names[selector.get_support()]

    classifier = model.named_steps["classifier"]
    coefficients = classifier.coef_[0]

    coef_table = pd.DataFrame(
        {
            "feature": feature_names,
            "coefficient": coefficients,
            "abs_coefficient": abs(coefficients),
        }
    ).sort_values("abs_coefficient", ascending=False)

    plot_df = coef_table.head(int(top_n)).sort_values("coefficient")
    colors = ["steelblue" if value < 0 else "crimson" for value in plot_df["coefficient"]]

    plt.figure(figsize=(10, 8))
    plt.barh(plot_df["feature"], plot_df["coefficient"], color=colors)
    plt.axvline(0, color="black", linewidth=1)
    plt.xlabel("Logistic regression coefficient")
    plt.ylabel("Feature")
    plt.title(f"Top {len(plot_df)} Features by Absolute Coefficient")
    plt.tight_layout()
    plt.show()

    return coef_table
