import os
from etl_pipeline.assets.bronze import *
from dagster import AssetIn, OutputContext, Definitions, job, graph, multi_asset
from pyspark.sql import SparkSession
from resources.spark_io_manager import get_spark_session
from resources import spark_io_manager
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
import gc  # Import the garbage collection module

import polars as pl

COMPUTE_KIND = "PostgreSQL"
LAYER = "Silver"


@asset(
    io_manager_key="psql_io_manager",
    ins={"bronze_olist_customers_dataset": AssetIn(key_prefix=["bronze", "ecommerce"])},
    key_prefix=["silver", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def olist_customers_dataset(context, bronze_olist_customers_dataset: pl.DataFrame):
    bronze_olist_customers_dataset = bronze_olist_customers_dataset.drop_nulls()
    bronze_olist_customers_dataset = bronze_olist_customers_dataset.unique()

    return Output(
        value=bronze_olist_customers_dataset,
        metadata={
            "table": "olist_customers_dataset",
            "row_count": bronze_olist_customers_dataset.shape[0],
            "column_count": len(bronze_olist_customers_dataset.columns),
            "columns": bronze_olist_customers_dataset.columns,
        },
    )


@asset(
    io_manager_key="psql_io_manager",
    ins={"bronze_olist_orders_dataset": AssetIn(key_prefix=["bronze", "ecommerce"])},
    key_prefix=["silver", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def olist_orders_dataset(context, bronze_olist_orders_dataset: pl.DataFrame):
    bronze_olist_orders_dataset = bronze_olist_orders_dataset.drop_nulls()
    bronze_olist_orders_dataset = bronze_olist_orders_dataset.unique()

    return Output(
        value=bronze_olist_orders_dataset,
        metadata={
            "table": "olist_orders_dataset",
            "row_count": bronze_olist_orders_dataset.shape[0],
            "column_count": len(bronze_olist_orders_dataset.columns),
            "columns": bronze_olist_orders_dataset.columns,
        },
    )


@asset(
    io_manager_key="psql_io_manager",
    ins={"bronze_olist_products_dataset": AssetIn(key_prefix=["bronze", "ecommerce"])},
    key_prefix=["silver", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def olist_products_dataset(context, bronze_olist_products_dataset: pl.DataFrame):
    bronze_olist_products_dataset = bronze_olist_products_dataset.drop_nulls()
    bronze_olist_products_dataset = bronze_olist_products_dataset.unique()

    return Output(
        value=bronze_olist_products_dataset,
        metadata={
            "table": "olist_products_dataset",
            "row_count": bronze_olist_products_dataset.shape[0],
            "column_count": len(bronze_olist_products_dataset.columns),
            "columns": bronze_olist_products_dataset.columns,
        },
    )


@asset(
    io_manager_key="psql_io_manager",
    ins={
        "bronze_olist_order_items_dataset": AssetIn(key_prefix=["bronze", "ecommerce"])
    },
    key_prefix=["silver", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def olist_order_items_dataset(context, bronze_olist_order_items_dataset: pl.DataFrame):

    return Output(
        value=bronze_olist_order_items_dataset,
        metadata={
            "table": "olist_order_items_dataset",
            "row_count": bronze_olist_order_items_dataset.shape[0],
            "column_count": len(bronze_olist_order_items_dataset.columns),
            "columns": bronze_olist_order_items_dataset.columns,
        },
    )


@asset(
    io_manager_key="psql_io_manager",
    ins={
        "bronze_olist_order_payments_dataset": AssetIn(
            key_prefix=["bronze", "ecommerce"]
        )
    },
    key_prefix=["silver", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def olist_order_payments_dataset(
    context, bronze_olist_order_payments_dataset: pl.DataFrame
):
    bronze_olist_order_payments_dataset = (
        bronze_olist_order_payments_dataset.drop_nulls()
    )
    bronze_olist_order_payments_dataset = bronze_olist_order_payments_dataset.unique()

    return Output(
        value=bronze_olist_order_payments_dataset,
        metadata={
            "table": "olist_order_payments_dataset",
            "row_count": bronze_olist_order_payments_dataset.shape[0],
            "column_count": len(bronze_olist_order_payments_dataset.columns),
            "columns": bronze_olist_order_payments_dataset.columns,
        },
    )


@asset(
    io_manager_key="psql_io_manager",
    ins={"bronze_olist_sellers_dataset": AssetIn(key_prefix=["bronze", "ecommerce"])},
    key_prefix=["silver", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def olist_sellers_dataset(context, bronze_olist_sellers_dataset: pl.DataFrame):
    bronze_olist_sellers_dataset = bronze_olist_sellers_dataset.drop_nulls()
    bronze_olist_sellers_dataset = bronze_olist_sellers_dataset.unique()

    return Output(
        value=bronze_olist_sellers_dataset,
        metadata={
            "table": "olist_sellers_dataset",
            "row_count": bronze_olist_sellers_dataset.shape[0],
            "column_count": len(bronze_olist_sellers_dataset.columns),
            "columns": bronze_olist_sellers_dataset.columns,
        },
    )


@asset(
    io_manager_key="psql_io_manager",
    ins={
        "bronze_olist_geolocation_dataset": AssetIn(key_prefix=["bronze", "ecommerce"])
    },
    key_prefix=["silver", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def olist_geolocation_dataset(context, bronze_olist_geolocation_dataset: pl.DataFrame):
    bronze_olist_geolocation_dataset = bronze_olist_geolocation_dataset.drop_nulls()
    bronze_olist_geolocation_dataset = bronze_olist_geolocation_dataset.unique()

    return Output(
        value=bronze_olist_geolocation_dataset,
        metadata={
            "table": "olist_geolocation_dataset",
            "row_count": bronze_olist_geolocation_dataset.shape[0],
            "column_count": len(bronze_olist_geolocation_dataset.columns),
            "columns": bronze_olist_geolocation_dataset.columns,
        },
    )


@asset(
    io_manager_key="psql_io_manager",
    ins={"bronze_olist_reviews_dataset": AssetIn(key_prefix=["bronze", "ecommerce"])},
    key_prefix=["silver", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def olist_order_reviews_dataset(
    context, bronze_olist_reviews_dataset: pl.DataFrame
):  # Change the argument name here
    bronze_olist_reviews_dataset = bronze_olist_reviews_dataset.drop_nulls()
    bronze_olist_reviews_dataset = bronze_olist_reviews_dataset.unique()

    return Output(
        value=bronze_olist_reviews_dataset,
        metadata={
            "table": "olist_order_reviews_dataset",
            "row_count": bronze_olist_reviews_dataset.shape[0],
            "column_count": len(bronze_olist_reviews_dataset.columns),
            "columns": bronze_olist_reviews_dataset.columns,
        },
    )
