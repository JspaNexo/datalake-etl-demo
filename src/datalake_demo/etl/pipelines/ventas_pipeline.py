"""Casos de uso independientes de configuración, formatos y servicios."""

import logging
from contextlib import contextmanager
from uuid import uuid4

from datalake_demo.domain.sales import aggregate_sales, clean_sales
from datalake_demo.etl.models import BronzeResult, Execution, GoldResult, Quality, SilverResult
from datalake_demo.etl.ports import Clock, LakeRepository, PublicationRepository, SalesSource


class SalesPipeline:
    def __init__(self, dataset: str, source_type: str, source: SalesSource, lake: LakeRepository,
                 publications: PublicationRepository, clock: Clock, logger: logging.Logger,
                 correlation_id: str | None = None):
        self.dataset, self.source_type = dataset, source_type
        self.source, self.lake, self.publications = source, lake, publications
        self.clock, self.logger = clock, logger
        self.correlation_id = correlation_id or str(uuid4())

    def _properties(self, execution: Execution | None = None) -> dict:
        props = {"SourceType": self.source_type, "Dataset": self.dataset}
        if execution:
            props.update(ExecutionId=execution.execution_id, Revision=execution.revision)
        return props

    def _log(self, stage: str, execution: Execution, **properties):
        self.logger.info("Etapa %s completada", stage, extra={
            "correlation_id": self.correlation_id, "stage": stage,
            "properties": {**self._properties(execution), **properties},
        })

    @contextmanager
    def _stage(self, stage: str, execution: Execution | None = None):
        try:
            if execution and (execution.dataset, execution.source_type) != (self.dataset, self.source_type):
                raise ValueError("La referencia pertenece a otra fuente")
            yield
        except Exception:
            self.logger.exception("Error en etapa %s", stage, extra={
                "correlation_id": self.correlation_id, "stage": stage,
                "properties": self._properties(execution),
            })
            raise

    def bronze(self, *, expected_sha256: str | None = None) -> BronzeResult:
        with self._stage("bronze"):
            execution = self.publications.begin(self.dataset, self.correlation_id, self.source_type, self.clock())
            result = self.publications.captured(execution)
            if result is None:
                snapshot = self.source.read_snapshot()
                if expected_sha256 is not None and snapshot.sha256 != expected_sha256:
                    raise ValueError("La fuente cambio despues de la deteccion")
                result = self.publications.bind_bronze(self.lake.write_bronze(execution, snapshot))
            self._log("bronze", execution, Rows=result.rows, SourceKey=result.artifact.key,
                      SourceSha256=result.artifact.sha256)
            return result

    def silver(self, bronze: BronzeResult) -> SilverResult:
        execution = bronze.execution
        with self._stage("silver", execution):
            raw = self.lake.read_bronze(bronze)
            sales, rejected = clean_sales(raw)
            if raw and not sales:
                raise ValueError("No hay ventas validas. Revisar los datos de entrada.")
            quality: Quality = {
                "archivo_bronze": bronze.artifact.key,
                "filas_bronze": len(raw), "filas_silver": len(sales), "filas_rechazadas": len(rejected),
                "generado_utc": execution.created_utc,
            }
            result = self.lake.write_silver(bronze, sales, rejected, quality)
            self._log("silver", execution, ValidRows=result.rows, RejectedRows=result.rejected_rows)
            return result

    def gold(self, silver: SilverResult) -> GoldResult:
        execution = silver.bronze.execution
        with self._stage("gold", execution):
            result = self.lake.write_gold(silver, aggregate_sales(self.lake.read_silver(silver)))
            self._log("gold", execution, Groups=result.groups, RevenueBs=result.revenue_bs)
            return result

    def publish_gold(self, gold: GoldResult):
        with self._stage("sql", gold.execution):
            prepared = self.lake.prepare(gold)
            rows, quality = self.lake.read_publication(prepared)
            result = self.publications.publish(prepared, rows, quality)
            self._log("sql", gold.execution, Rows=result.rows, PublicationStatus=result.status,
                      ManifestKey=result.manifest.key)
            return result

    def run(self) -> dict:
        self.correlation_id = str(uuid4())
        bronze = self.bronze()
        silver = self.silver(bronze)
        gold = self.gold(silver)
        return {"bronze": bronze, "silver": silver, "gold": gold, "sql": self.publish_gold(gold)}
