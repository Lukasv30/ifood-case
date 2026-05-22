import os
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional


# Official NYC TLC public files are hosted in this CloudFront path.
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DEFAULT_YEAR = 2023
DEFAULT_MONTHS = [1, 2, 3, 4, 5]


# Unity Catalog Volume configuration.
CATALOG_NAME = "workspace"
SCHEMA_NAME = "ifood_case"
VOLUME_NAME = "case_files"


# Use dbfs:/Volumes for Spark/dbutils paths.
DBFS_VOLUME_BASE_PATH = f"dbfs:/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

# Use /Volumes for direct Python file writes.
LOCAL_VOLUME_BASE_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"


LANDING_DBFS_BASE_PATH = (
    f"{DBFS_VOLUME_BASE_PATH}/landing/nyc_tlc/yellow_taxi"
)

LANDING_LOCAL_BASE_PATH = (
    f"{LOCAL_VOLUME_BASE_PATH}/landing/nyc_tlc/yellow_taxi"
)


def ensure_unity_catalog_volume(spark) -> None:
    """
    Cria o Unity Catalog e o volume para armanezamos a landing
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS "
        f"{CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}"
    )


def build_tlc_url(year: int, month: int) -> str:
    """
    Cria o arquivo em .parquet para a URL NYC TLC for Yellow Taxi trip data.

    Exemplo:
    https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
    """
    return f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def build_landing_path(year: int, month: int) -> str:
    """
    Cria o caminho para o DBFS usando Spark e dbutils.
    """
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"

    return (
        f"{LANDING_DBFS_BASE_PATH}/"
        f"year={year}/"
        f"month={month:02d}/"
        f"{file_name}"
    )


def build_landing_local_path(year: int, month: int) -> str:
    """
    Cria a arquitetura de pastas na landing separando por ano/mês com Python.
    """
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"

    return (
        f"{LANDING_LOCAL_BASE_PATH}/"
        f"year={year}/"
        f"month={month:02d}/"
        f"{file_name}"
    )


def dbfs_path_exists(dbutils, path: str) -> bool:
    """
    Verificação de volume, vê se ele está criado.
    """
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def download_to_volume(source_url: str, target_local_path: str) -> int:
    """
    Faz download do endpoint publico e extrai os .parquets

    Evita o uso de /tmp e o dbutils.fs.cp do sistema de arquivos local, para não pararmos em bloqueio do ambiente Databricks Serverless / Unity Catalog.
    """
    os.makedirs(os.path.dirname(target_local_path), exist_ok=True)

    request = urllib.request.Request(
        source_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 "
                "(Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        },
    )

    with urllib.request.urlopen(request) as response:
        with open(target_local_path, "wb") as output_file:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output_file.write(chunk)

    return os.path.getsize(target_local_path)


def ingest_file_to_landing(
    dbutils,
    year: int,
    month: int,
    overwrite: bool = False,
) -> Dict:
    """
    Baixe o arquivo mensal do NYC TLC Yellow Taxi e armazena na landing.
    """
    source_url = build_tlc_url(year, month)
    landing_dbfs_path = build_landing_path(year, month)
    landing_local_path = build_landing_local_path(year, month)

    ingestion_started_at = datetime.now(timezone.utc).isoformat()

    if dbfs_path_exists(dbutils, landing_dbfs_path) and not overwrite:
        return {
            "year": year,
            "month": month,
            "source_url": source_url,
            "landing_path": landing_dbfs_path,
            "status": "skipped_already_exists",
            "file_size_bytes": None,
            "ingestion_started_at": ingestion_started_at,
            "ingestion_finished_at": datetime.now(timezone.utc).isoformat(),
        }

    try:
        if dbfs_path_exists(dbutils, landing_dbfs_path) and overwrite:
            dbutils.fs.rm(landing_dbfs_path, recurse=False)

        file_size_bytes = download_to_volume(
            source_url=source_url,
            target_local_path=landing_local_path,
        )

        status = "success"

    except Exception as exc:
        file_size_bytes = None
        status = f"failed: {type(exc).__name__}: {str(exc)}"

        # Best effort cleanup for partially written files.
        try:
            if os.path.exists(landing_local_path):
                os.remove(landing_local_path)
        except Exception:
            pass

    return {
        "year": year,
        "month": month,
        "source_url": source_url,
        "landing_path": landing_dbfs_path,
        "status": status,
        "file_size_bytes": file_size_bytes,
        "ingestion_started_at": ingestion_started_at,
        "ingestion_finished_at": datetime.now(timezone.utc).isoformat(),
    }


def ingest_landing(
    dbutils,
    year: int = DEFAULT_YEAR,
    months: Optional[List[int]] = None,
    overwrite: bool = False,
) -> List[Dict]:
    
    if months is None:
        months = DEFAULT_MONTHS

    results = []

    for month in months:
        result = ingest_file_to_landing(
            dbutils=dbutils,
            year=year,
            month=month,
            overwrite=overwrite,
        )
        results.append(result)

    return results
