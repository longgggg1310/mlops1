import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
import polars as pl

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    try:
        yield client
    except Exception:
        raise None
    
def make_bucket(client: Minio, bucket_name):
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket {bucket_name} already exists.")

class MinIOIOManager(IOManager):

    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):

        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file_{}.parquet".format(
            "_".join(context.asset_key.path),
        )
        if context.has_asset_partitions:
            partition_str = str(table) + "_" + context.asset_partition_key
            return os.path.join(key, f"{partition_str}.parquet"), tmp_file_path        
        else:
            return f"{key}.parquet", tmp_file_path
        
        
    def handle_output(self, context: OutputContext, obj: pl.DataFrame) :
    # convert to parquet format
        key_name, tmp_file_path = self._get_path(context)
        obj.write_parquet(tmp_file_path)

        # upload to MinIO
        try:
            bucket_name = self._config.get("bucket")
            print("6961 ", bucket_name)
            with connect_minio(self._config) as client:

                # Make bucket if not exist
                make_bucket(client, bucket_name)
                client.fput_object(bucket_name, key_name, tmp_file_path)
                context.add_output_metadata({"path": key_name, "tmp": tmp_file_path})
                os.remove(tmp_file_path)
        except Exception as e:
            raise None
        
    def load_input(self, context: InputContext) -> pl.DataFrame:
        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)
        print("longvu123",bucket_name, key_name, tmp_file_path )
        try:
            with connect_minio(self._config) as client:
                # Make bucket if not exist.
                make_bucket(client=client, bucket_name=bucket_name)
            
                context.log.info(f"(MinIO load_input) from key_name: {key_name}")

                client.fget_object(bucket_name, key_name, tmp_file_path)
                df_data = pl.read_parquet(tmp_file_path)
                context.log.info(
                    f"(MinIO load_input) Got polars dataframe with shape: {df_data.shape}"
                )
                return df_data
        except Exception as e:
            context.log.error(f"(MinIO load_input) Error occurred: {str(e)}")
            raise RuntimeError(f"Failed to load input due to: {str(e)}") from e
