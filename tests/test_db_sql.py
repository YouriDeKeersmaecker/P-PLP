from p_plp.db.sql_utils import sql_age_expression, sql_date_add_days, sql_date_subtract_days


def test_sql_date_add_days_builds_expected_fragment():
    assert sql_date_add_days("t.index_date", "risk_end_days") == "(t.index_date + :risk_end_days)"


def test_sql_date_subtract_days_builds_expected_fragment():
    assert sql_date_subtract_days("b.index_date", "lookback_days") == "(b.index_date - :lookback_days)"


def test_sql_age_expression_mentions_birth_and_index_fields():
    expr = sql_age_expression("b.index_date")

    assert "b.index_date" in expr
    assert "year_of_birth" in expr
    assert "month_of_birth" in expr
    assert "day_of_birth" in expr
