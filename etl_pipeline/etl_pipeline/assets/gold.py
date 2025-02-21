from etl_pipeline.assets.silver import *
from dagster import AssetOut, multi_asset, InputContext
from resources.minio_io_manager import MinIOIOManager
from resources.psql_io_manager import PostgreSQLIOManager
from resources.minio_io_manager import MinIOIOManager
from pyspark.sql import functions as F
import pyarrow as pa


COMPUTE_KIND = "Postgres"
LAYER = "Gold"


@asset(
    io_manager_key="psql_io_manager",
    ins={
        "silver_dim_customers": AssetIn(key_prefix=["silver", "ecommerce"]),
    },
    key_prefix=["gold", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def gold_dim_customers(context, silver_dim_customers: DataFrame):

    arrow_table = silver_dim_customers._collect_as_arrow()
    polars_df = pl.from_arrow(arrow_table)

    context.log.debug(f"Got Polars DataFrame with shape: {polars_df.shape}")

    # Explicitly release memory
    del silver_dim_customers
    gc.collect()

    return Output(
        polars_df,
        metadata={
            "table": "gold_dim_customers",
            "row_count": polars_df.shape[0],
            "column_count": len(polars_df.columns),
            "columns": polars_df.columns,
        },
    )
