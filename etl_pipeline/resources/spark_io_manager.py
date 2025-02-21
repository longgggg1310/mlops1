import os
from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame
from typing import Union
from contextlib import contextmanager
from datetime import datetime


@contextmanager
def get_spark_session(config, run_id="Spark IO Manager"):

    executor_memory = "4g" if run_id != "Spark IO Manager" else "1500m"
    try:
        spark = (
            SparkSession.builder.master("local[*]")
            .appName(run_id)
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", executor_memory)
            .config("spark.cores.max", "4")
            .config("spark.executor.cores", "2")
            .config(
                "spark.jars",
                "../jars/postgresql-42.7.1.jar,"
                "../jars/delta-storage-2.2.0.jar,"
                "../jars/aws-java-sdk-1.12.367.jar,"
                "../jars/s3-2.18.41.jar,"
                "../jars/aws-java-sdk-bundle-1.12.262.jar,"
                "../jars/hadoop-aws-3.3.4.jar,"
                "../jars/delta-core_2.12-2.2.0.jar",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.driver.extraClassPath", "../jars/postgresql-42.7.1.jar")
            .config("spark.executor.extraClassPath", "../jars/postgresql-42.7.1.jar")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minio")
            .config("spark.hadoop.fs.s3a.secret.key", "minio123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.connection.ssl.enabled", "true")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            .getOrCreate()
        )

        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")


class SparkIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"), "-".join(context.asset_key.path)
        )
        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            dt_format = "%Y%m%d%H%M%S"

            partition_str = start.strftime(dt_format) + "_" + end.strftime(dt_format)

            return os.path.join(key, f"{partition_str}.pq"), tmp_file_path
        else:
            return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: DataFrame):
        file_path = "s3a://lakehouse/" + "/".join(context.asset_key.path)

        if context.has_partition_key:
            file_path += f"/{context.partition_key}"
        context.log.debug(f"(Spark handle_output) File path: {file_path}")
        file_name = str(context.asset_key.path[-1])
        context.log.debug(f"(Spark handle_output) File name: {file_name}")

        try:
            obj.write.mode("overwrite").parquet(file_path)
            context.log.debug(f"Saved {file_name} to {file_path}")
        except Exception as e:
            raise Exception(f"(Spark handle_output) Error while writing output: {e}")

    def load_input(self, context: InputContext) -> DataFrame:
        """
        Load input from s3a (aka minIO) from parquet file to spark.sql.DataFrame
        """

        # E.g context.asset_key.path: ['silver', 'goodreads', 'book']
        context.log.debug(f"Loading input from {context.asset_key.path}...")
        file_path = "s3a://lakehouse/" + "/".join(context.asset_key.path)
        check_partition = (context.metadata or {}).get("partition", True)
        if check_partition == True:
            if context.has_partition_key:
                file_path += f"/{context.partition_key}"
        full_load = (context.metadata or {}).get("full_load", False)
        try:
            with get_spark_session(self._config) as spark:
                df = None
                if full_load:
                    df = (
                        spark.read.format("parquet")
                        .options(header=True, inferSchema=False)
                        .load(file_path + "/*")
                    )
                else:
                    df = spark.read.parquet(file_path)
                context.log.debug(f"Loaded {df.count()} rows from {file_path}")
                return df
        except Exception as e:
            raise Exception(f"Error while loading input: {e}")
