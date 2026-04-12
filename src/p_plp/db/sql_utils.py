def sql_date_add_days(date_expression: str, days_param: str) -> str:
    """Build a SQL fragment that adds a parameterized number of days to a date."""

    return f"({date_expression} + :{days_param})"


def sql_date_subtract_days(date_expression: str, days_param: str) -> str:
    """Build a SQL fragment that subtracts a parameterized number of days from a date."""

    return f"({date_expression} - :{days_param})"


def sql_age_expression(index_date_expression: str) -> str:
    """Build a SQL expression that estimates age on an index date from OMOP person fields."""

    birth_month = "coalesce(nullif(p.month_of_birth, 0), 6)"
    birth_day = "coalesce(nullif(p.day_of_birth, 0), 15)"
    return f"""
    (
        extract(year from {index_date_expression}) - p.year_of_birth
        - case
            when {birth_month} > extract(month from {index_date_expression}) then 1
            when {birth_month} = extract(month from {index_date_expression})
             and {birth_day} > extract(day from {index_date_expression}) then 1
            else 0
        end
    )::int
    """
