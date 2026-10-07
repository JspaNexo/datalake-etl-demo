"""Lectura de ventas operacionales; usa una conexion distinta a la de Gold."""

from datalake_demo.config.settings import Settings

SALES_QUERY = """
    SELECT d.detalle_id AS venta_id, v.fecha, c.nombre AS ciudad,
           p.nombre AS producto, d.cantidad, d.precio_unitario
    FROM operacion.detalles_venta AS d
    JOIN operacion.ventas AS v ON v.venta_id = d.venta_id
    JOIN operacion.clientes AS cliente ON cliente.cliente_id = v.cliente_id
    JOIN operacion.ciudades AS c ON c.ciudad_id = cliente.ciudad_id
    JOIN operacion.productos AS p ON p.producto_id = d.producto_id
    WHERE v.estado = 'CONFIRMADA'
    ORDER BY d.detalle_id
"""


class PostgresSalesSource:
    def __init__(self, settings: Settings):
        self.settings = settings

    def connect(self):
        import psycopg

        return psycopg.connect(
            host=self.settings.source_postgres_host, port=self.settings.source_postgres_port,
            dbname=self.settings.source_postgres_db, user=self.settings.source_postgres_user,
            password=self.settings.source_postgres_password, connect_timeout=10,
        )

    def read_sales(self) -> list[tuple]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute(SALES_QUERY)
                return cursor.fetchall()

    def read_snapshot(self):
        from datalake_demo.infra.csv_source import snapshot_from_records

        return snapshot_from_records(self.read_sales())
