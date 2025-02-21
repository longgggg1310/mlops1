from dagster import IOManager, InputContext, OutputContext
from contextlib import contextmanager
from datetime import datetime
from pyspark.sql import DataFrame
import psycopg2.extras
import pandas as pd  # Import Pandas for converting Polars DataFrame
import polars as pl
from sqlalchemy import create_engine
import os


@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )

    print("123", conn_info)

    try:
        yield conn_info
    except Exception:
        raise


class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context):
        layer, schema, table = context.asset_key.path
        key = f"{layer}/{schema}/{table}"
        tmp_dir_path = f"/tmp/{layer}/{schema}/"

        os.makedirs(tmp_dir_path, exist_ok=True)
        tmp_file_path = f"{tmp_dir_path}{table}.parquet"

        return f"{key}.parquet", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        table = context.asset_key.path[-1]
        schema = context.asset_key.path[-2]

        print("00000 ", table, schema)
        with connect_psql(self._config) as engine:
            obj.write_database(
                table, engine, if_table_exists="replace", engine="sqlalchemy"
            )

        print("Write successfully!")

    def load_input(self, context: InputContext) -> pl.DataFrame:
        pass
