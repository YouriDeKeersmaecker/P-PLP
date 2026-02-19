import pandas as pd
from sqlalchemy import text
from enum import Enum
from .config import CDM_SCHEMA

class CdmTable(str, Enum):
    PERSON = "person"
    CONDITION_OCCURRENCE = "condition_occurrence"
    DRUG_EXPOSURE = "drug_exposure"
    VISIT_OCCURRENCE = "visit_occurrence"
    OBSERVATION_PERIOD = "observation_period"
    MEASUREMENT = "measurement"
    PROCEDURE_OCCURRENCE = "procedure_occurrence"
    DEATH = "death"


def get_cdm_table(
    engine,
    table: CdmTable,
    limit: int | None = 20
) -> pd.DataFrame:

    sql = f"SELECT * FROM {CDM_SCHEMA}.{table.value}"

    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)