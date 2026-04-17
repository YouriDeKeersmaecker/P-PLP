from __future__ import annotations


def compute_cha2ds2vasc(row):
    score = 0

    score += int(row["heart_failure"])
    score += int(row["hypertension"])

    if row["age"] >= 75:
        score += 2
    elif row["age"] >= 65:
        score += 1

    score += int(row["diabetes"])

    if int(row["ischemic_stroke"]):
        score += 2

    # gender in your query is probably OMOP gender_concept_id
    if int(row["gender"]) == 8532:  # female
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
