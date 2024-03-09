import shutil

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from config import Buckets
from tests.test_data import db_1

HIVE_PATH = Buckets.LOCAL.value

databases = [db_1]


def setup_hive(spark: SparkSession = None):
    logger.info("Setting up Hive")

    for database in databases:
        module_elements = [
            v
            for v in dir(database)
            if not v.startswith("__") and type(getattr(database, v)) is dict
        ]
        for element in module_elements:
            _ = getattr(database, element)

            db = _["database"]
            table_name = _["table_name"]
            schema = _["schema"]
            data = _["data"]

            spark.sql(
                f"""
                create database if not exists {db} comment 'test data' location '{HIVE_PATH}/{db}'
                """
            )

            df = spark.createDataFrame(data=data, schema=schema)
            df.write.saveAsTable(f"{db}.{table_name}")
            logger.info(f"Created hive table: {db}.{table_name}")


def drop_tables():
    """
    Dropping all or particular hive table(s) metadata.
    """
    drop_dir = HIVE_PATH

    logger.info(f"dropping table(s) in {drop_dir}")

    try:
        shutil.rmtree(drop_dir)
        logger.info(f"Succesfully dropped tables in {drop_dir}")
    except (AnalysisException, FileNotFoundError) as exp_msg:
        logger.warning(exp_msg)
