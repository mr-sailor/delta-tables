import pytest


@pytest.mark.unit
def test_load_table(spark_session):
    spark_session.table("db_1.table_1").show()
