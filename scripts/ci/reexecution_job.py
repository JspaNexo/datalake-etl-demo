"""Job reconstruible para verificar la reejecución nativa de Dagster en CI."""

from dagster import in_process_executor

from datalake_demo.jobs.definitions import defs


def csv_job():
    return defs.resolve_job_def("etl_ventas").with_executor_def(in_process_executor)
