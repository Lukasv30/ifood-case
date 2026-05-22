from functools import reduce
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


CATALOG_NAME = "workspace"
SCHEMA_NAME = "ifood_case"
VOLUME_NAME = "case_files"

VOLUME_BASE_PATH = f"dbfs:/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

LANDING_BASE_PATH = (
    f"{VOLUME_BASE_PATH}/landing/nyc_tlc/yellow_taxi"
)

RAW_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.raw_yellow_taxi_trips"

REQUIRED_COLUMNS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


def build_landing_file_path(year: int, month: str) -> str:
    """
    Build the monthly landing file path.

    The landing layer stores the original NYC TLC Parquet files organized
    by year and month.
    """
    return (
        f"{LANDING_BASE_PATH}/"
        f"year={year}/"
        f"month={month}/"
        f"yellow_tripdata_{year}-{month}.parquet"
    )


def read_landing_month(
    spark: SparkSession,
    year: int,
    month: str,
) -> DataFrame:
    """
    Read one monthly file from the landing zone and apply a controlled schema.

    The raw files may have physical schema differences across months.
    For this reason, each monthly file is read independently and then
    normalized before the union operation.
    """
    source_file = build_landing_file_path(year, month)

    df = spark.read.parquet(source_file)

    return (
        df.select(
            F.col("VendorID").cast("int").alias("VendorID"),
            F.col("passenger_count").cast("double").alias("passenger_count"),
            F.col("total_amount").cast("double").alias("total_amount"),
            F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
            F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
        )
        .withColumn("source_year", F.lit(year))
        .withColumn("source_month", F.lit(int(month)))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("raw_loaded_at", F.current_timestamp())
    )


def build_raw_dataframe(
    spark: SparkSession,
    year: int,
    months: List[str],
) -> DataFrame:
    """
    Build the Raw DataFrame by reading all selected monthly landing files.
    """
    monthly_dfs = [
        read_landing_month(
            spark=spark,
            year=year,
            month=month,
        )
        for month in months
    ]

    return reduce(
        lambda left, right: left.unionByName(right),
        monthly_dfs,
    )


def write_raw_table(
    spark: SparkSession,
    raw_df: DataFrame,
    mode: str = "overwrite",
) -> None:
    """
    Persist the Raw DataFrame as a Delta table in Unity Catalog.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

    (
        raw_df
        .write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .partitionBy("source_year", "source_month")
        .saveAsTable(RAW_TABLE_NAME)
    )


def create_raw(
    spark: SparkSession,
    year: int,
    months: List[str],
    mode: str = "overwrite",
) -> DataFrame:
    """
    Build and persist the Raw table.
    """
    raw_df = build_raw_dataframe(
        spark=spark,
        year=year,
        months=months,
    )

    write_raw_table(
        spark=spark,
        raw_df=raw_df,
        mode=mode,
    )

    return raw_df