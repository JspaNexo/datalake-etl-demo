"""Prueba de integracion para los datos originales: ejecutar dentro de Docker."""

from decimal import Decimal

from dagster import DagsterInstance, DagsterRunStatus, Definitions, build_sensor_context

from datalake_demo.config.settings import GOLD_KEY, QUALITY_KEY, REJECTED_KEY, SILVER_KEY, Settings
from datalake_demo.etl.extract import extract_source
from datalake_demo.infra.minio_storage import MinioStorage
from datalake_demo.infra.postgres_storage import PostgresStorage
from datalake_demo.jobs.definitions import defs, ventas_csv_sensor


def check(condition, description):
    if not condition:
        raise AssertionError(description)
    print(f"OK: {description}")


def verify():
    settings = Settings.from_env()
    lake = MinioStorage(settings)
    warehouse = PostgresStorage(settings)
    Definitions.validate_loadable(defs)
    job = defs.resolve_job_def("etl_ventas")
    for active_job in (job, defs.resolve_job_def("__ASSET_JOB")):
        with DagsterInstance.ephemeral() as waiting_instance:
            waiting_instance.create_run_for_job(active_job, status=DagsterRunStatus.STARTED)
            waiting_tick = ventas_csv_sensor.evaluate_tick(build_sensor_context(
                instance=waiting_instance, definitions=defs,
            ))
            check(not waiting_tick.run_requests and "en curso" in waiting_tick.skip_message,
                  f"sensor espera mientras {active_job.name} esta activo")
    with DagsterInstance.get() as instance:
        first_tick = ventas_csv_sensor.evaluate_tick(build_sensor_context(instance=instance, definitions=defs))
        check(not first_tick.run_requests and bool(first_tick.skip_message),
              "sensor espera una segunda lectura estable del CSV")
        second_tick = ventas_csv_sensor.evaluate_tick(build_sensor_context(
            instance=instance, definitions=defs, cursor=first_tick.cursor,
        ))
        check(len(second_tick.run_requests) == 1, "sensor solicita una ejecucion para el contenido nuevo")
        request = second_tick.run_requests[0]
        check(request.tags["source_sha256"] == extract_source(settings.source_path).sha256,
              "sensor identifica exactamente el contenido que se procesara")
        for attempt in (1, 2):
            result = job.execute_in_process(instance=instance, tags=request.tags if attempt == 1 else None)
            check(result.success, f"ejecucion Dagster {attempt} completada")
        unchanged_tick = ventas_csv_sensor.evaluate_tick(build_sensor_context(
            instance=instance, definitions=defs, cursor=second_tick.cursor,
        ))
        check(not unchanged_tick.run_requests, "sensor no vuelve a ejecutar el mismo contenido")

    source = extract_source(settings.source_path)
    bronze_key = source.bronze_key
    check(lake.get_bytes(bronze_key) == source.payload, "bronze conserva los bytes originales")
    # Comprueba que existe exactamente un objeto para ESTE contenido; permite otros originales.
    objects = lake.client.list_objects_v2(Bucket=lake.bucket, Prefix=bronze_key)
    check(objects.get("KeyCount") == 1, "repetir el flujo reutiliza el objeto bronze")
    silver = lake.get_parquet(SILVER_KEY)
    gold = lake.get_parquet(GOLD_KEY)
    rejected = lake.get_json(REJECTED_KEY)
    quality = lake.get_json(QUALITY_KEY)
    check(len(silver) == 2008 and len(rejected) == 6, "silver: 2008 ventas validas y 6 rechazos")
    check(len({r["venta_id"] for r in silver}) == 2008, "silver sin IDs duplicados")
    check(quality["filas_bronze"] == quality["filas_silver"] + quality["filas_rechazadas"] == 2014,
          "conteos de calidad conciliados")
    check(len(gold) == 1608 and sum(r["ingresos"] for r in gold) == Decimal("221634.82"),
          "gold: 1608 grupos y Bs 221634.82 en Parquet")

    with warehouse.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*), SUM(ventas), SUM(unidades), SUM(ingresos) FROM gold.ventas_diarias")
            check(cursor.fetchone() == (1608, 2008, 21478, Decimal("221634.82")),
                  "PostgreSQL conserva los totales tras dos ejecuciones")
            cursor.execute("SELECT fecha, ciudad, producto, ventas, unidades, ingresos FROM gold.ventas_diarias ORDER BY fecha, ciudad, producto")
            expected = [(r["fecha"], r["ciudad"], r["producto"], r["ventas"], r["unidades"], r["ingresos"]) for r in gold]
            check(cursor.fetchall() == expected, "PostgreSQL y gold en MinIO coinciden fila por fila")
            cursor.execute("SELECT COUNT(*), MIN(filas_bronze), MIN(filas_silver), MIN(filas_rechazadas) FROM gold.calidad")
            check(cursor.fetchone() == (1, 2014, 2008, 6), "informe de calidad publicado sin duplicarse")
    print("Prueba de integracion completada.")


if __name__ == "__main__":
    verify()
