"""Contrato CSV externo y captura exacta de sus bytes."""

import csv
import hashlib
import io
from pathlib import Path

from datalake_demo.etl.models import SourceSnapshot
from datalake_demo.infra.file_storage import read_file

CSV_COLUMNS = ("venta_id", "fecha", "ciudad", "producto", "cantidad", "precio_unitario")


def read_sales(payload: bytes) -> list[dict]:
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8-sig")), strict=True)
    if reader.fieldnames != list(CSV_COLUMNS):
        raise ValueError(f"El CSV debe tener estas columnas, en este orden: {CSV_COLUMNS}")
    try:
        return list(reader)
    except csv.Error as error:
        raise ValueError("CSV mal formado") from error


def snapshot_from_bytes(payload: bytes) -> SourceSnapshot:
    return SourceSnapshot(payload, read_sales(payload), hashlib.sha256(payload).hexdigest())


def snapshot_from_records(records) -> SourceSnapshot:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(CSV_COLUMNS)
    writer.writerows(records)
    return snapshot_from_bytes(buffer.getvalue().encode("utf-8"))


class CsvSalesSource:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def read_snapshot(self) -> SourceSnapshot:
        return snapshot_from_bytes(read_file(self.path))
