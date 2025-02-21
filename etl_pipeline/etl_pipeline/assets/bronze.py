from dagster import (
    asset,
    AssetIn,
    Output,
    DailyPartitionsDefinition,
    AssetOut,
    multi_asset,
)

import polars as pl


COMPUTE_KIND = "MySQL"
LAYER = "Bronze"


# # partition lỗi, lúc chạy k load data vào postgres nếu partition
@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    # partitions_def=DailyPartitionsDefinition(start_date="2018-01-01")
)
def bronze_olist_orders_dataset(context) -> Output[pl.DataFrame]:

    sql_stm = "SELECT * FROM olist_orders_dataset"
    # try:
    #     partion_year_str = context.asset_partition_key_for_output()
    #     partition_by = "order_purchase_timestamp"
    #     sql_stm += f" WHERE {partition_by} = {partion_year_str}"
    #     context.log.info(f"Partition by {partition_by} = {partion_year_str}")
    # except Exception:
    #     context.log.info("No partition key found, full load data")

    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    print(pd_data)
    return Output(
        pd_data,
        metadata={
            "table": "bronze_olist_orders_dataset",
            "records count": len(pd_data),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_olist_customers_dataset(context) -> Output[pl.DataFrame]:
    sql_stm = "SELECT * FROM olist_customers_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    print(pd_data)
    return Output(
        pd_data,
        metadata={
            "table": "olist_customers_dataset",
            "records count": len(pd_data),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_olist_products_dataset(context) -> Output[pl.DataFrame]:
    sql_stm = "SELECT * FROM olist_products_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
            "table": "olist_products_dataset",
            "records count": len(pd_data),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_olist_order_items_dataset(context) -> Output[pl.DataFrame]:
    sql_stm = "SELECT * FROM olist_order_items_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    print("123123123", pd_data.head())
    return Output(
        pd_data,
        metadata={
            "table": "olist_order_items_dataset",
            "records count": len(pd_data),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_olist_order_payments_dataset(context) -> Output[pl.DataFrame]:
    sql_stm = "SELECT * FROM olist_order_payments_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
            "table": "olist_order_payments_dataset",
            "records count": len(pd_data),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_olist_sellers_dataset(context) -> Output[pl.DataFrame]:
    sql_stm = "SELECT * FROM olist_sellers_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
            "table": "olist_sellers_dataset",
            "records count": len(pd_data),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_olist_geolocation_dataset(context) -> Output[pl.DataFrame]:
    sql_stm = "SELECT * FROM olist_geolocation_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
            "table": "olist_geolocation_dataset",
            "records count": len(pd_data),
        },
    )


@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecommerce"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_olist_reviews_dataset(context) -> Output[pl.DataFrame]:
    sql_stm = "SELECT * FROM olist_order_reviews_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)

    return Output(
        pd_data,
        metadata={
            "table": "olist_order_reviews_dataset",
            "records count": len(pd_data),
        },
    )
