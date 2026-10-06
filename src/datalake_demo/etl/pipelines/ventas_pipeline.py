"""Coordina extract -> transform -> load para las capas bronze, silver y gold."""

import logging
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from datalake_demo.config.logging import get_logger
from datalake_demo.config.settings import Settings
from datalake_demo.etl.extract import extract_bronze, extract_gold, extract_silver, extract_source
from datalake_demo.etl.load import load_bronze, load_gold, load_serving, load_silver
from datalake_demo.etl.transform import aggregate_sales, transform_to_silver
from datalake_demo.infra.minio_storage import MinioStorage
from datalake_demo.infra.postgres_storage import PostgresStorage


class SalesPipeline:
    def __init__(self, settings: Settings, lake, warehouse, logger: logging.Logger, correlation_id: str | None = None):
        self.settings = settings
        self.lake = lake
        self.warehouse = warehouse
        self.logger = logger
        self.correlation_id = correlation_id or str(uuid4())

    @classmethod
    def from_settings(cls, settings: Settings, correlation_id: str | None = None) -> "SalesPipeline":
        return cls(settings, MinioStorage(settings), PostgresStorage(settings), get_logger(settings), correlation_id)

    def _log_stage(self, message: str, *args, stage: str, **properties):
        self.logger.info(message, *args, extra={
            "correlation_id": self.correlation_id, "stage": stage, "properties": properties,
        })

    @contextmanager
    def _stage(self, stage: str):
        try:
            yield
        except Exception:
            self.logger.exception("Error en etapa %s", stage, extra={
                "correlation_id": self.correlation_id, "stage": stage,
            })
            raise

    def bronze(self, source: str | Path | None = None) -> dict:
        with self._stage("bronze"):
            extracted = extract_source(source if source is not None else self.settings.source_path)
            result = load_bronze(self.lake, extracted)
            self._log_stage("Bronze completado: %s filas; archivo %s", result["filas"], result["key"],
                            stage="bronze", Rows=result["filas"], SourceKey=result["key"])
            return result

    def silver(self, bronze_result: dict) -> dict:
        with self._stage("silver"):
            raw = extract_bronze(self.lake, bronze_result["key"])
            batch = transform_to_silver(raw, bronze_result["key"])
            result = load_silver(self.lake, batch)
            self._log_stage("Silver completado: %s ventas validas; %s rechazos", result["filas"], result["rechazadas"],
                            stage="silver", ValidRows=result["filas"], RejectedRows=result["rechazadas"])
            return result

    def gold(self, silver_result: dict) -> dict:
        with self._stage("gold"):
            sales = extract_silver(self.lake, silver_result["key"])
            result = load_gold(self.lake, aggregate_sales(sales))
            self._log_stage("Gold completado: %s grupos; Bs %s", result["grupos"], result["ingresos_bs"],
                            stage="gold", Groups=result["grupos"], RevenueBs=result["ingresos_bs"])
            return result

    def publish_gold(self, gold_result: dict) -> dict:
        with self._stage("sql"):
            rows, quality = extract_gold(self.lake, gold_result["key"])
            result = load_serving(self.warehouse, rows, quality)
            self._log_stage("Publicacion SQL completada: %s filas en %s", result["filas"], result["tabla"],
                            stage="sql", Rows=result["filas"], Table=result["tabla"])
            return result

    def run(self, source: str | Path | None = None) -> dict:
        self.correlation_id = str(uuid4())
        self._log_stage("Iniciando ETL de ventas; entorno %s", self.settings.app_env, stage="pipeline")
        bronze_result = self.bronze(source)
        silver_result = self.silver(bronze_result)
        gold_result = self.gold(silver_result)
        publication = self.publish_gold(gold_result)
        self._log_stage("ETL de ventas finalizado", stage="pipeline")
        return {"bronze": bronze_result, "silver": silver_result, "gold": gold_result, "sql": publication}
