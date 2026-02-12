from p_plp.db.utils import read_table

def test_read_table_basic():
    df = read_table("person", "cdm_synthea10", limit=1)
    assert len(df) <= 1
