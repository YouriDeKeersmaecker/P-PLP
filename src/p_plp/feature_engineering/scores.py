from __future__ import annotations


def compute_cha2ds2vasc(row):
    score = 0

    score += row["heart_failure"]
    score += row["hypertension"]

    if row["age"] >= 75:
        score += 2
    elif row["age"] >= 65:
        score += 1

    score += row["diabetes"]

    if row["ischemic_stroke"]:
        score += 2

    score += row["peripheral_vascular_disease"]

    if row["gender"] == "female":
        score += 1

    return score


def compute_has_bled(row):
    score = 0

    score += row["hypertension"]
    score += row["ischemic_stroke"]
    score += row["bleeding_history"]

    if row["age"] > 65:
        score += 1

    score += row["drinking_history"]

    return score
