import pytest
from pyspark.sql import SparkSession

from config import Environments
from delta_tables.apps.context import SparkContextCreator
from tests.setup import drop_all_tables, setup_hive


@pytest.fixture(scope="session", autouse=False)
def spark_session() -> SparkSession:
    """
    Fixture for SparkSession
    """
    spark = SparkContextCreator(
        app_name="fixture_spark_session",
        environment=Environments.LOCAL,
    ).get_session()

    drop_all_tables()
    setup_hive(spark)
    yield spark
    spark.sparkContext._gateway.shutdown_callback_server()
    spark.stop()
