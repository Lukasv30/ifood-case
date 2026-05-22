from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


CATALOG_NAME = "workspace"
SCHEMA_NAME = "ifood_case"

SILVER_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_yellow_taxi_trips"

GOLD_TRIPS_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.gold_yellow_taxi_trips"
GOLD_MONTHLY_AVG_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.gold_avg_total_amount_by_month"
GOLD_MAY_HOURLY_PASSENGERS_TABLE_NAME = (
    f"{CATALOG_NAME}.{SCHEMA_NAME}.gold_avg_passenger_count_may_by_hour"
)


def read_silver_table(spark: SparkSession) -> DataFrame:
    """
    Lê a tabela de prata para criar a camada de ouro.
    """
    return spark.table(SILVER_TABLE_NAME)


def build_gold_trips_dataframe(silver_df: DataFrame) -> DataFrame:
    """
    Cria uma tabela Gold com um valor de negócio voltado para o nível de viagem otimizada para negócios.

    Esta tabela mantém os campos obrigatórios do caso e atributos analíticos úteis de data/hora criados na camada Silver.
    """
    return (
        silver_df
        .select(
            F.col("VendorID"),
            F.col("passenger_count"),
            F.col("total_amount"),
            F.col("tpep_pickup_datetime"),
            F.col("tpep_dropoff_datetime"),
            F.col("pickup_date"),
            F.col("pickup_year"),
            F.col("pickup_month"),
            F.col("pickup_day"),
            F.col("pickup_hour"),
            F.col("trip_duration_minutes"),
        )
    )


def build_monthly_avg_total_amount_dataframe(silver_df: DataFrame) -> DataFrame:
    """
    Resposta para pergunta de negócios 1:

    Qual é o valor total médio recebido em um mês, considerando todas as viagens de táxi amarelo?
    """
    return (
        silver_df
        .groupBy(
            F.col("pickup_year"),
            F.col("pickup_month"),
        )
        .agg(
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.count("*").alias("trip_count"),
        )
        .orderBy("pickup_year", "pickup_month")
    )


def build_may_hourly_avg_passenger_count_dataframe(silver_df: DataFrame) -> DataFrame:
    """
    Resposta para pergunta de negócios 2:

    Qual é a média de passageiros por hora do dia em maio, considerando todas as viagens de táxi amarelo?
    """
    return (
        silver_df
        .filter(F.col("pickup_year") == 2023)
        .filter(F.col("pickup_month") == 5)
        .groupBy(
            F.col("pickup_hour"),
        )
        .agg(
            F.round(F.avg("passenger_count"), 2).alias("avg_passenger_count"),
            F.count("*").alias("trip_count"),
        )
        .orderBy("pickup_hour")
    )


def write_delta_table(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    
    (
        df
        .write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def create_gold(
    spark: SparkSession,
    mode: str = "overwrite",
) -> dict:
    """
    Cria as tabelas da camada Gold.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

    silver_df = read_silver_table(spark)

    gold_trips_df = build_gold_trips_dataframe(silver_df)

    monthly_avg_df = build_monthly_avg_total_amount_dataframe(silver_df)

    may_hourly_passengers_df = build_may_hourly_avg_passenger_count_dataframe(silver_df)

    write_delta_table(
        df=gold_trips_df,
        table_name=GOLD_TRIPS_TABLE_NAME,
        mode=mode,
    )

    write_delta_table(
        df=monthly_avg_df,
        table_name=GOLD_MONTHLY_AVG_TABLE_NAME,
        mode=mode,
    )

    write_delta_table(
        df=may_hourly_passengers_df,
        table_name=GOLD_MAY_HOURLY_PASSENGERS_TABLE_NAME,
        mode=mode,
    )

    return {
        "gold_trips_df": gold_trips_df,
        "monthly_avg_df": monthly_avg_df,
        "may_hourly_passengers_df": may_hourly_passengers_df,
    }
