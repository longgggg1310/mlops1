import polars as pl 
from dagster import Output, AssetIn, multi_asset, AssetOut, asset

from pyspark.sql import DataFrame

@asset(
    ins={
        "covid_stats_by_day": AssetIn(key_prefix=["gold", "medical"])
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "public"],
    group_name="warehouse_layer",

)

def warehouse_covid19_daily_stats(covid_stats_by_day: DataFrame) :        
    return Output(
        covid_stats_by_day,
        metadata={
            "schema": "public",
            "table": "covid19_daily_stats",
            "records counts": covid_stats_by_day.count(),
        },
    )



@asset(
    ins={
        "covid19_stats_continent": AssetIn(key_prefix=["gold", "medical"])
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "public"],
    group_name="warehouse_layer"
)

def warehouse_covid19_continent_stats(covid19_stats_continent: DataFrame) :   
         
    return Output(
        covid19_stats_continent,
        metadata={
            "schema": "public",
            "table": "covid19_stats_continent",
            "records counts": covid19_stats_continent.count(),
        },
    )