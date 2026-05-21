import os
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional
S
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DEFAULT_YEAR = 2023
DEFAULT_MONTHS = [1, 2, 3, 4, 5]


CATALOG_NAME = "workspace"
SCHEMA_NAME = "ifood_case"
VOLUME_NAME = "case_files"

VOLUME_BASE_PATH = f"dbfs:/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

LANDING_BASE_PATH = (
    f"{VOLUME_BASE_PATH}/landing/nyc_tlc/yellow_taxi"
)

LOCAL_TMP_DIR = "/tmp/ifood_case_landing"


def build_tlc_url(year: int, month: int) -> str:
    """
    Cria a url de extração com o endpoint disponibilizado
    """
    return f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def build_landing_path(year: int, month: int) -> str:
    """
    Cria landing com os arquivos .parquet divididos por pastas com estilo "year/month".
    """
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"

    return (
        f"{LANDING_BASE_PATH}/"
        f"year={year}/"
        f"month={month:02d}/"
        f"{file_name}"
    )


def dbfs_path_exists(dbutils, path: str) -> bool:
    """
    Verifica o caminho no DBFS.
    """
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def download_to_local(url: str, local_path: str) -> int:
    """
    Baixa o arquivo .parquet e produz o log de de progresso e bytes baixados.
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    urllib.request.urlretrieve(url, local_path)

    return os.path.getsize(local_path)


def ingest_file_to_landing(
    dbutils,
    year: int,
    month: int,
    overwrite: bool = False,
) -> Dict:
    """
    Faz o download dos arquivos por mês, iterando o dowload por mês, verifica se o arquivo já existe e cria a pasta na landing.
    """
    source_url = build_tlc_url(year, month)
    landing_path = build_landing_path(year, month)

    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    local_path = f"{LOCAL_TMP_DIR}/{file_name}"

    ingestion_started_at = datetime.now(timezone.utc).isoformat()

    if dbfs_path_exists(dbutils, landing_path) and not overwrite:
        return {
            "year": year,
            "month": month,
            "source_url": source_url,
            "landing_path": landing_path,
            "status": "skipped_already_exists",
            "file_size_bytes": None,
            "ingestion_started_at": ingestion_started_at,
            "ingestion_finished_at": datetime.now(timezone.utc).isoformat(),
        }

    try:
        file_size_bytes = download_to_local(source_url, local_path)

        landing_dir = "/".join(landing_path.split("/")[:-1])
        dbutils.fs.mkdirs(landing_dir)

        dbutils.fs.cp(
            f"file:{local_path}",
            landing_path,
            recurse=False,
        )

        status = "success"

    except Exception as exc:
        file_size_bytes = None
        status = f"failed: {str(exc)}"

    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

    return {
        "year": year,
        "month": month,
        "source_url": source_url,
        "landing_path": landing_path,
        "status": status,
        "file_size_bytes": file_size_bytes,
        "ingestion_started_at": ingestion_started_at,
        "ingestion_finished_at": datetime.now(timezone.utc).isoformat(),
    }


def ingest_landing(
    dbutils,
    year: int = DEFAULT_YEAR,
    months: List[int] = DEFAULT_MONTHS,
    overwrite: bool = False,
) -> List[Dict]:

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