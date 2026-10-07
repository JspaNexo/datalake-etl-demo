"""Adaptadores Dagster: delegan en los mismos casos de uso que la consola."""

from dataclasses import asdict

from dagster import (
    AssetExecutionContext, AssetSelection, DagsterRunStatus, DefaultSensorStatus,
    Definitions, RunRequest, RunsFilter, SensorEvaluationContext, SkipReason,
    asset, define_asset_job, resource, sensor,
)

from datalake_demo.bootstrap import build_pipeline, build_source
from datalake_demo.config.settings import Settings
from datalake_demo.etl.models import BronzeResult, GoldResult, SilverResult
from datalake_demo.jobs.run_io import run_reference_io_manager
from datalake_demo.jobs.source_monitor import evaluate_content_change, evaluate_source_change

etl_ventas = define_asset_job("etl_ventas", selection=AssetSelection.groups("ventas"))
etl_ventas_db = define_asset_job("etl_ventas_db", selection=AssetSelection.groups("ventas_db"))


def has_active_run(context, job_name):
    statuses = [DagsterRunStatus.NOT_STARTED, DagsterRunStatus.QUEUED, DagsterRunStatus.STARTING,
                DagsterRunStatus.STARTED, DagsterRunStatus.CANCELING]
    return any(context.instance.get_runs(filters=RunsFilter(job_name=name, statuses=statuses), limit=1)
               for name in (job_name, "__ASSET_JOB"))


@sensor(job=etl_ventas, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING)
def ventas_csv_sensor(context: SensorEvaluationContext):
    settings = Settings.from_env()
    change = evaluate_source_change(settings.source_path, context.cursor,
                                    run_in_progress=has_active_run(context, "etl_ventas"))
    context.update_cursor(change.cursor)
    if change.reason:
        return SkipReason(change.reason)
    return RunRequest(run_key=change.run_key, tags={
        "source_sha256": change.sha256, "source_path": str(settings.source_path), "etl_trigger": "csv_change",
    })


@sensor(job=etl_ventas_db, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING)
def ventas_db_sensor(context: SensorEvaluationContext):
    active = has_active_run(context, "etl_ventas_db")
    settings = Settings.from_env().for_database_source()
    digest = None if active else build_source(settings).read_snapshot().sha256
    change = evaluate_content_change(digest, context.cursor, run_in_progress=active,
                                     source_label="PostgreSQL", run_key_prefix="ventas_db", stable_readings=1)
    context.update_cursor(change.cursor)
    if change.reason:
        return SkipReason(change.reason)
    return RunRequest(run_key=change.run_key, tags={
        "observed_source_sha256": change.sha256,
        "source_path": f"postgres:{settings.source_postgres_db}.operacion", "etl_trigger": "database_change",
    })


@resource
def sales_pipeline_resource(context):
    return build_pipeline(Settings.from_env(), context.run_id)


@resource
def sales_db_pipeline_resource(context):
    return build_pipeline(Settings.from_env().for_database_source(), context.run_id)


def metadata(context, result):
    # Solo datos JSON en la interfaz; el IO manager conserva los resultados tipados.
    context.add_output_metadata({"result": asdict(result)})
    return result


@asset(group_name="ventas", required_resource_keys={"sales_pipeline"})
def bronze_ventas(context: AssetExecutionContext) -> BronzeResult:
    return metadata(context, context.resources.sales_pipeline.bronze(expected_sha256=context.run.tags.get("source_sha256")))


@asset(group_name="ventas", required_resource_keys={"sales_pipeline"})
def silver_ventas(context: AssetExecutionContext, bronze_ventas: BronzeResult) -> SilverResult:
    return metadata(context, context.resources.sales_pipeline.silver(bronze_ventas))


@asset(group_name="ventas", required_resource_keys={"sales_pipeline"})
def gold_ventas(context: AssetExecutionContext, silver_ventas: SilverResult) -> GoldResult:
    return metadata(context, context.resources.sales_pipeline.gold(silver_ventas))


@asset(group_name="ventas", required_resource_keys={"sales_pipeline"})
def publicar_gold_sql(context: AssetExecutionContext, gold_ventas: GoldResult):
    return metadata(context, context.resources.sales_pipeline.publish_gold(gold_ventas))


@asset(group_name="ventas_db", required_resource_keys={"sales_db_pipeline"})
def bronze_ventas_db(context: AssetExecutionContext) -> BronzeResult:
    # La huella del sensor SQL es informativa: capturamos un snapshot nuevo y consistente.
    return metadata(context, context.resources.sales_db_pipeline.bronze())


@asset(group_name="ventas_db", required_resource_keys={"sales_db_pipeline"})
def silver_ventas_db(context: AssetExecutionContext, bronze_ventas_db: BronzeResult) -> SilverResult:
    return metadata(context, context.resources.sales_db_pipeline.silver(bronze_ventas_db))


@asset(group_name="ventas_db", required_resource_keys={"sales_db_pipeline"})
def gold_ventas_db(context: AssetExecutionContext, silver_ventas_db: SilverResult) -> GoldResult:
    return metadata(context, context.resources.sales_db_pipeline.gold(silver_ventas_db))


@asset(group_name="ventas_db", required_resource_keys={"sales_db_pipeline"})
def publicar_gold_sql_db(context: AssetExecutionContext, gold_ventas_db: GoldResult):
    return metadata(context, context.resources.sales_db_pipeline.publish_gold(gold_ventas_db))


defs = Definitions(
    assets=[bronze_ventas, silver_ventas, gold_ventas, publicar_gold_sql,
            bronze_ventas_db, silver_ventas_db, gold_ventas_db, publicar_gold_sql_db],
    jobs=[etl_ventas, etl_ventas_db], sensors=[ventas_csv_sensor, ventas_db_sensor],
    resources={"sales_pipeline": sales_pipeline_resource, "sales_db_pipeline": sales_db_pipeline_resource,
               "io_manager": run_reference_io_manager},
)
