"""Compara el snapshot relacional con SQL y verifica el aislamiento del CSV."""

from datalake_demo.config.settings import Settings
from datalake_demo.infra.postgres_source import PostgresSalesSource
from datalake_demo.infra.postgres_storage import PostgresStorage
from datalake_demo.jobs.definitions import ventas_db_sensor
from datalake_demo.jobs.verification import check, run_twice, verify_current, write_summary

REFERENCE_QUERY = """
    SELECT COUNT(*), COUNT(DISTINCT (v.fecha,c.nombre,p.nombre)),
           COALESCE(SUM(d.cantidad),0), COALESCE(SUM(d.cantidad*d.precio_unitario),0)
    FROM operacion.ventas v
    JOIN operacion.clientes cl ON cl.cliente_id=v.cliente_id
    JOIN operacion.ciudades c ON c.ciudad_id=cl.ciudad_id
    JOIN operacion.detalles_venta d ON d.venta_id=v.venta_id
    JOIN operacion.productos p ON p.producto_id=d.producto_id
    WHERE v.estado='CONFIRMADA'
"""


def verify():
    csv = Settings.from_env()
    settings = csv.for_database_source()
    reader = PostgresSalesSource(settings)
    with reader.connect() as connection, connection.cursor() as cursor:
        cursor.execute(REFERENCE_QUERY)
        count, groups, units, revenue = cursor.fetchone()
        cursor.execute("SELECT COUNT(*) FROM operacion.detalles_venta")
        original_details = cursor.fetchone()[0]
    before = PostgresStorage(csv).current(csv.dataset)
    before_stats = verify_current(csv)
    runs = run_twice(settings, "etl_ventas_db", ventas_db_sensor, 1)
    actual = verify_current(settings)
    expected = dict(source_rows=count, valid_rows=count, rejected_rows=0, groups=groups,
                    units=int(units), revenue_bs=format(revenue, ".2f"))
    check(actual == expected, "resultados concilian con SQL operacional")
    check(PostgresStorage(csv).current(csv.dataset) == before and verify_current(csv) == before_stats,
          "la publicacion del CSV permanece intacta")
    with reader.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM operacion.detalles_venta")
        check(cursor.fetchone()[0] == original_details, "origen sin modificaciones")
    write_summary("db-summary.json", {**actual, "runs": runs})


if __name__ == "__main__":
    verify()
