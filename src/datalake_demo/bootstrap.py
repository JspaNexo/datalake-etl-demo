"""Único lugar de construcción de las dependencias de los casos de uso."""

from dataclasses import replace

from datalake_demo.config.logging import get_logger
from datalake_demo.config.settings import Settings
from datalake_demo.etl.pipelines.ventas_pipeline import SalesPipeline
from datalake_demo.etl.workers.log_worker import LogWorker
from datalake_demo.infra.csv_source import CsvSalesSource
from datalake_demo.infra.minio_storage import MinioStorage
from datalake_demo.infra.postgres_source import PostgresSalesSource
from datalake_demo.infra.postgres_storage import PostgresStorage
from datalake_demo.utils.time_utils import utc_now_iso


def build_source(settings: Settings):
    return PostgresSalesSource(settings) if settings.source_type == "postgres" else CsvSalesSource(settings.source_path)


def build_pipeline(settings: Settings, correlation_id: str | None = None, *, logger=None) -> SalesPipeline:
    return SalesPipeline(settings.dataset, settings.source_type, build_source(settings), MinioStorage(settings),
                         PostgresStorage(settings), utc_now_iso, logger or get_logger(settings), correlation_id)


def build_log_worker(settings: Settings) -> LogWorker:
    worker_settings = replace(settings, app_name=f"{settings.app_name}_worker",
                              log_file_name=settings.worker_log_file_name)
    return LogWorker(get_logger(worker_settings), utc_now_iso, settings.worker_interval_seconds)
