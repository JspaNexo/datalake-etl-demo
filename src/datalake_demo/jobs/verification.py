"""Comprobaciones de integración reutilizadas por los verificadores de CI."""

import json
from decimal import Decimal
from pathlib import Path

from dagster import DagsterInstance, Definitions, build_sensor_context

from datalake_demo.infra.minio_storage import MinioStorage
from datalake_demo.infra.postgres_storage import PostgresStorage
from datalake_demo.jobs.definitions import defs


def check(condition, description):
    if not condition:
        raise AssertionError(description)
    print("OK: " + description, flush=True)


def run_twice(settings, job_name, sensor, stable_readings):
    Definitions.validate_loadable(defs)
    job = defs.resolve_job_def(job_name)
    with DagsterInstance.get() as instance:
        first = sensor.evaluate_tick(build_sensor_context(instance=instance, definitions=defs))
        if stable_readings == 2:
            check(not first.run_requests, "CSV espera una segunda lectura estable")
            tick = sensor.evaluate_tick(build_sensor_context(instance=instance, definitions=defs, cursor=first.cursor))
        else:
            tick = first
        check(len(tick.run_requests) == 1, "sensor solicita el snapshot")
        request = tick.run_requests[0]
        runs, publications = [], []
        for attempt in (1, 2):
            result = job.execute_in_process(instance=instance, tags=request.tags if attempt == 1 else None)
            check(result.success, f"{job_name}: ejecucion {attempt}")
            runs.append(result.run_id)
            publications.append(PostgresStorage(settings).current(settings.dataset))
        unchanged = sensor.evaluate_tick(build_sensor_context(instance=instance, definitions=defs, cursor=tick.cursor))
        check(not unchanged.run_requests, "sensor omite contenido ya enviado")
    lake = MinioStorage(settings)
    first, second = [lake.read_prepared(p.manifest) for p in publications]
    check(first.gold.silver.bronze.artifact == second.gold.silver.bronze.artifact, "Bronze reutilizado por contenido")
    check(first.gold.artifact.key != second.gold.artifact.key, "Gold conserva versiones distintas")
    lake.read_publication(first)
    check(publications[0].revision < publications[1].revision, "revisiones crecientes")
    return runs


def verify_current(settings):
    from psycopg import sql
    lake, publisher = MinioStorage(settings), PostgresStorage(settings)
    current = publisher.current(settings.dataset)
    check(current is not None, "publicacion vigente registrada")
    prepared = lake.read_prepared(current.manifest)
    gold, quality = lake.read_publication(prepared)
    silver = lake.read_silver(prepared.gold.silver)
    rejected = lake.get_json(prepared.gold.silver.rejected.key)
    bronze = lake.read_bronze(prepared.gold.silver.bronze)
    check(len(bronze) == len(silver) + len(rejected), "Bronze, Silver y rechazos conciliados")
    check(current.execution_id == prepared.gold.execution.execution_id
          and current.revision == prepared.gold.execution.revision, "version vigente corresponde al manifiesto")
    check(sum(r["importe"] for r in silver) == sum(r["ingresos"] for r in gold), "Silver y Gold concilian importes")
    with publisher.connect() as connection, connection.cursor() as cursor:
        cursor.execute(sql.SQL("SELECT fecha,ciudad,producto,ventas,unidades,ingresos FROM {} ORDER BY fecha,ciudad,producto")
                       .format(sql.Identifier("gold", settings.gold_table)))
        expected = [(r["fecha"], r["ciudad"], r["producto"], r["ventas"], r["unidades"], r["ingresos"]) for r in gold]
        check(cursor.fetchall() == expected, "SQL y Parquet coinciden fila por fila")
        cursor.execute(sql.SQL("SELECT archivo_bronze,filas_bronze,filas_silver,filas_rechazadas FROM {}")
                       .format(sql.Identifier("gold", settings.quality_table)))
        check(cursor.fetchall() == [(quality["archivo_bronze"], len(bronze), len(silver), len(rejected))],
              "SQL y calidad corresponden al mismo snapshot")
    return dict(source_rows=len(bronze), valid_rows=len(silver), rejected_rows=len(rejected),
                groups=len(gold), units=sum(r["unidades"] for r in gold),
                revenue_bs=str(sum((r["ingresos"] for r in gold), Decimal("0.00"))))


def write_summary(name, summary):
    path = Path("/app/artifacts") / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
