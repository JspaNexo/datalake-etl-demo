"""Publica cada resultado utilizando los adaptadores de infraestructura."""

from decimal import Decimal

from datalake_demo.config.settings import GOLD_KEY, QUALITY_KEY, REJECTED_KEY, SILVER_KEY
from datalake_demo.domain.models import ExtractedSales, SilverBatch


def load_bronze(lake, source: ExtractedSales) -> dict:
    lake.ensure_bucket()
    lake.put_original(source.bronze_key, source.payload)
    return {"key": source.bronze_key, "filas": len(source.rows), "sha256": source.sha256}


def load_silver(lake, batch: SilverBatch) -> dict:
    lake.put_parquet(SILVER_KEY, batch.sales, "silver")
    lake.put_json(REJECTED_KEY, batch.rejected)
    lake.put_json(QUALITY_KEY, batch.quality)
    return {"key": SILVER_KEY, "filas": len(batch.sales), "rechazadas": len(batch.rejected)}


def load_gold(lake, rows: list[dict]) -> dict:
    lake.put_parquet(GOLD_KEY, rows, "gold")
    return {"key": GOLD_KEY, "grupos": len(rows),
            "ingresos_bs": str(sum((row["ingresos"] for row in rows), Decimal("0.00")))}


def load_serving(warehouse, rows: list[dict], quality: dict) -> dict:
    warehouse.replace_gold(rows, quality)
    return {"tabla": "gold.ventas_diarias", "filas": len(rows)}

