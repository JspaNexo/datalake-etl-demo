"""Lee la fuente de cada etapa, sin limpiar ni publicar los datos."""

import csv
import hashlib
import io
from pathlib import Path

from datalake_demo.config.settings import QUALITY_KEY
from datalake_demo.domain.models import CSV_COLUMNS, ExtractedSales
from datalake_demo.infra.file_storage import read_file


def read_sales(payload: bytes) -> list[dict]:
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig")))
    if reader.fieldnames != list(CSV_COLUMNS):
        raise ValueError(f"El CSV debe tener estas columnas, en este orden: {CSV_COLUMNS}")
    return list(reader)


def extract_source(source: str | Path) -> ExtractedSales:
    payload = read_file(source)
    return ExtractedSales(payload=payload, rows=read_sales(payload), sha256=hashlib.sha256(payload).hexdigest())


def extract_bronze(lake, key: str) -> list[dict]:
    return read_sales(lake.get_bytes(key))


def extract_silver(lake, key: str) -> list[dict]:
    return lake.get_parquet(key)


def extract_gold(lake, key: str) -> tuple[list[dict], dict]:
    return lake.get_parquet(key), lake.get_json(QUALITY_KEY)

