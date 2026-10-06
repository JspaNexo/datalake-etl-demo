"""Prueba de integracion para los datos originales: ejecutar dentro de Docker."""

from decimal import Decimal

from dagster import DagsterInstance

from datalake_demo.config.settings import GOLD_KEY, QUALITY_KEY, REJECTED_KEY, SILVER_KEY, Settings
from datalake_demo.etl.extract import extract_source
from datalake_demo.infra.minio_storage import MinioStorage
from datalake_demo.infra.postgres_storage import PostgresStorage
from datalake_demo.jobs.definitions import defs


def check(condition, description):
    if not condition:
        raise AssertionError(description)
    print(f"OK: {description}")


def verify():
    settings = Settings.from_env()
    lake = MinioStorage(settings)
    warehouse = PostgresStorage(settings)
    job = defs.resolve_job_def("etl_ventas")
    with DagsterInstance.get() as instance:
        for attempt in (1, 2):
            result = job.execute_in_process(instance=instance)
            check(result.success, f"ejecucion Dagster {attempt} completada")

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
    check(len(silver) == 8 and len(rejected) == 6, "silver: 8 ventas validas y 6 rechazos")
    check(len({r["venta_id"] for r in silver}) == 8, "silver sin IDs duplicados")
    check(quality["filas_bronze"] == quality["filas_silver"] + quality["filas_rechazadas"] == 14,
          "conteos de calidad conciliados")
    check(len(gold) == 7 and sum(r["ingresos"] for r in gold) == Decimal("356.00"),
          "gold: 7 grupos y Bs 356.00 en Parquet")

    with warehouse.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*), SUM(ventas), SUM(unidades), SUM(ingresos) FROM gold.ventas_diarias")
            check(cursor.fetchone() == (7, 8, 22, Decimal("356.00")),
                  "PostgreSQL conserva los totales tras dos ejecuciones")
            cursor.execute("SELECT fecha, ciudad, producto, ventas, unidades, ingresos FROM gold.ventas_diarias ORDER BY fecha, ciudad, producto")
            expected = [(r["fecha"], r["ciudad"], r["producto"], r["ventas"], r["unidades"], r["ingresos"]) for r in gold]
            check(cursor.fetchall() == expected, "PostgreSQL y gold en MinIO coinciden fila por fila")
            cursor.execute("SELECT COUNT(*), MIN(filas_bronze), MIN(filas_silver), MIN(filas_rechazadas) FROM gold.calidad")
            check(cursor.fetchone() == (1, 14, 8, 6), "informe de calidad publicado sin duplicarse")
    print("Prueba de integracion completada.")


if __name__ == "__main__":
    verify()
