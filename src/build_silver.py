from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


CATALOG_NAME = "workspace"
SCHEMA_NAME = "ifood_case"

RAW_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.raw_yellow_taxi_trips"
SILVER_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_yellow_taxi_trips"


def read_raw_table(spark: SparkSession) -> DataFrame:
    """
    Read the Raw table created from the landing files.
    """
    return spark.table(RAW_TABLE_NAME)


def build_silver_dataframe(raw_df: DataFrame) -> DataFrame:
    """
    Build the Silver DataFrame.

    The Silver layer applies minimal data quality rules and creates
    analytical helper columns for downstream SQL/PySpark consumption.
    """
    return (
        raw_df
        .select(
            F.col("VendorID").cast("int").alias("VendorID"),
            F.col("passenger_count").cast("double").alias("passenger_count"),
            F.col("total_amount").cast("double").alias("total_amount"),
            F.col("tpep_pickup_datetime").cast("timestamp").alias("tpep_pickup_datetime"),
            F.col("tpep_dropoff_datetime").cast("timestamp").alias("tpep_dropoff_datetime"),
            F.col("source_year").cast("int").alias("source_year"),
            F.col("source_month").cast("int").alias("source_month"),
            F.col("source_file").cast("string").alias("source_file"),
            F.col("raw_loaded_at").cast("timestamp").alias("raw_loaded_at"),
        )
        .filter(F.col("VendorID").isNotNull())
        .filter(F.col("passenger_count").isNotNull())
        .filter(F.col("total_amount").isNotNull())
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("tpep_pickup_datetime") >= F.lit("2023-01-01"))
        .filter(F.col("tpep_pickup_datetime") < F.lit("2023-06-01"))
        .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("total_amount") >= 0)
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn("pickup_day", F.dayofmonth("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ) / 60,
        )
        .withColumn("silver_loaded_at", F.current_timestamp())
    )


def write_silver_table(
    spark: SparkSession,
    silver_df: DataFrame,
    mode: str = "overwrite",
) -> None:
    """
    Persist the Silver DataFrame as a Delta table in Unity Catalog.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

    (
        silver_df
        .write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .partitionBy("pickup_year", "pickup_month")
        .saveAsTable(SILVER_TABLE_NAME)
    )


def create_silver(
    spark: SparkSession,
    mode: str = "overwrite",
) -> DataFrame:
    """
    Build and persist the Silver table.
    """
    raw_df = read_raw_table(spark)

    silver_df = build_silver_dataframe(raw_df)

    write_silver_table(
        spark=spark,
        silver_df=silver_df,
        mode=mode,
    )

    return silver_df