from dagster import Definitions, load_assets_from_modules, file_relative_path
import os
from dotenv import load_dotenv

from . import assets
from resources.mysql_io_manager import MySQLIOManager
from resources.minio_io_manager import MinIOIOManager
from resources.spark_io_manager import SparkIOManager

from resources.psql_io_manager import PostgreSQLIOManager

load_dotenv()

PSQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "testing",
    "user": "admin",
    "password": "admin123",
}
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "testing",
    "user": "admin",
    "password": "admin123",
}
MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "lakehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}
# SPARK_CONFIG = {
#     "spark_master": os.getenv("SPARK_MASTER_URL"),
#     "spark_version": "3.3.3",
#     "hadoop_version": "3",
#     "endpoint_url": "localhost:9000",
#     "minio_access_key": "minio",
#     "minio_secret_key": "minio123",
# }
resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    # "spark_io_manager": SparkIOManager(SPARK_CONFIG),
}

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources=resources,
)
