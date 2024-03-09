import pytest
from delta_tables.tests.setup import drop_tables, setup_hive
from pyspark.sql import SparkSession

from config import Environments
from delta_tables.apps.context import SparkContextCreator


@pytest.fixture(scope="session", autouse=False)
def spark_session() -> SparkSession:
    """
    Fixture for SparkSession
    """
    _scf = SparkContextCreator(
        app_name="fixture_spark_session",
        environment=Environments.LOCAL,
    )
    spark = _scf.get_session()
    drop_tables()
    setup_hive(spark)
    yield spark
    spark.sparkContext._gateway.shutdown_callback_server()
    spark.stop()
