import pandas as pd
from sqlalchemy import text
from .engine import get_engine


def read_table(table_name, schema, limit=None):
    engine = get_engine()

    query = f"SELECT * FROM {schema}.{table_name}"

    if limit:
        query += f" LIMIT {limit}"

    with engine.connect() as connection:
        return pd.read_sql(text(query), connection)