"""Las mismas funciones Python, ejecutables desde la interfaz visual de Dagster."""

from dagster import (
    AssetExecutionContext, AssetSelection, DagsterRunStatus, DefaultSensorStatus,
    Definitions, RunRequest, RunsFilter, SensorEvaluationContext, SkipReason,
    asset, define_asset_job, resource, sensor,
)

from datalake_demo.config.settings import Settings
from datalake_demo.etl.pipelines.ventas_pipeline import SalesPipeline
from datalake_demo.jobs.source_monitor import evaluate_source_change

etl_ventas = define_asset_job("etl_ventas", selection=AssetSelection.groups("ventas"))


@sensor(job=etl_ventas, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING,
        description="Ejecutar ventas cuando cambia el CSV y su contenido permanece estable.")
def ventas_csv_sensor(context: SensorEvaluationContext):
    active_statuses = [
        DagsterRunStatus.NOT_STARTED, DagsterRunStatus.QUEUED, DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING,
    ]
    active = any(
        context.instance.get_runs(filters=RunsFilter(job_name=job_name, statuses=active_statuses), limit=1)
        for job_name in ("etl_ventas", "__ASSET_JOB")
    )
    settings = Settings.from_env()
    change = evaluate_source_change(settings.source_path, context.cursor, run_in_progress=active)
    context.update_cursor(change.cursor)
    if change.reason:
        return SkipReason(change.reason)
    return RunRequest(run_key=change.run_key, tags={
        "source_sha256": change.sha256,
        "source_path": str(settings.source_path),
        "etl_trigger": "csv_change",
    })


@resource
def sales_pipeline_resource(context):
    return SalesPipeline.from_settings(Settings.from_env(), correlation_id=context.run_id)


@asset(group_name="ventas", required_resource_keys={"sales_pipeline"}, description="Conservar el CSV original en MinIO, identificado por su SHA-256.")
def bronze_ventas(context: AssetExecutionContext) -> dict:
    result = context.resources.sales_pipeline.bronze(expected_sha256=context.run.tags.get("source_sha256"))
    context.add_output_metadata(result)
    return result


@asset(group_name="ventas", required_resource_keys={"sales_pipeline"}, description="Normalizar textos, validar tipos y separar filas rechazadas.")
def silver_ventas(context: AssetExecutionContext, bronze_ventas: dict) -> dict:
    result = context.resources.sales_pipeline.silver(bronze_ventas)
    context.add_output_metadata(result)
    return result


@asset(group_name="ventas", required_resource_keys={"sales_pipeline"}, description="Agregar ingresos y unidades por fecha, ciudad y producto.")
def gold_ventas(context: AssetExecutionContext, silver_ventas: dict) -> dict:
    result = context.resources.sales_pipeline.gold(silver_ventas)
    context.add_output_metadata(result)
    return result


@asset(group_name="ventas", required_resource_keys={"sales_pipeline"}, description="Publicar una copia de gold en PostgreSQL para consultas en SQLPad.")
def publicar_gold_sql(context: AssetExecutionContext, gold_ventas: dict) -> dict:
    result = context.resources.sales_pipeline.publish_gold(gold_ventas)
    context.add_output_metadata(result)
    return result


defs = Definitions(
    assets=[bronze_ventas, silver_ventas, gold_ventas, publicar_gold_sql],
    jobs=[etl_ventas],
    sensors=[ventas_csv_sensor],
    resources={"sales_pipeline": sales_pipeline_resource},
)
