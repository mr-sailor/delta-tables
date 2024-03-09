import os

from pyspark.sql import SparkSession

from config import Buckets, Environments


class SparkContextCreator:

    def __init__(self, app_name, environment):
        self.app_name = app_name
        self.environment = environment
        self.base_path = Buckets[environment.value].value
        self.hive_path = os.path.abspath(os.path.join(self.base_path, "hive-warehouse"))
        self.derby_path = os.path.abspath(os.path.join(self.base_path, "derby"))

    def get_session(self):
        if self.environment == Environments.LOCAL:
            return self.get_local_spark_session()

        if self.environment == Environments.PREPROD:
            return self.get_preprod_spark_session()

        if self.environment == Environments.PROD:
            return self.get_prod_spark_session()

    def get_local_spark_session(self):

        return (
            SparkSession.builder.appName(self.app_name)
            .master("local[3]")
            .config(
                "spark.driver.extraJavaOptions",
                f"-XX:+UseG1GC -Dderby.system.home={self.derby_path}",
            )
            .config("spark.driver.memory", "5G")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.rdd.compress", "false")
            .config("spark.shuffle.compress", "false")
            .config("spark.sql.warehouse.dir", f"{self.hive_path}")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.io.compression.codec", "snappy")
            .config(
                "spark.sql.analyzer.failAmbiguousSelfJoin", "false"
            )  # for spark 3 migration
            .config("spark.shuffle.compress", "true")
            .config("spark.scheduler.mode", "FAIR")
            .config("spark.speculation", "false")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config("spark.sql.crossJoin.enabled", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.files.ignoreMissingFiles", "true")
            .config("spark.files.ignoreCorruptFiles", "true")
            .config("spark.shuffle.io.retryWait", "60s")
            .config("spark.shuffle.io.maxRetries", "10")
            .config(
                "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored",
                "true",
            )
            .config("spark.hadoop.hive.exec.compress.output", "true")
            .config("spark.hadoop.hive.exec.parallel", "true")
            .config("spark.hadoop.parquet.enable.summary-metadata", "false")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .enableHiveSupport()
            .getOrCreate()
        )

    def get_prod_spark_session(self):
        return SparkSession.builder.getOrCreate()

    def get_preprod_spark_session(self):
        return self.get_preprod_spark_session()
