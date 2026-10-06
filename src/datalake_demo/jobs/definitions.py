"""Las mismas funciones Python, ejecutables desde la interfaz visual de Dagster."""

from dagster import AssetExecutionContext, AssetSelection, Definitions, asset, define_asset_job, resource

from datalake_demo.config.settings import Settings
from datalake_demo.etl.pipelines.ventas_pipeline import SalesPipeline


@resource
def sales_pipeline_resource(context):
    return SalesPipeline.from_settings(Settings.from_env(), correlation_id=context.run_id)


@asset(group_name="ventas", required_resource_keys={"sales_pipeline"}, description="Conservar el CSV original en MinIO, identificado por su SHA-256.")
def bronze_ventas(context: AssetExecutionContext) -> dict:
    result = context.resources.sales_pipeline.bronze()
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
    jobs=[define_asset_job("etl_ventas", selection=AssetSelection.groups("ventas"))],
    resources={"sales_pipeline": sales_pipeline_resource},
)
