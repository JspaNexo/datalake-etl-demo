"""Consultas y transacciones PostgreSQL, fuera de las reglas del ETL."""

from datalake_demo.config.settings import Settings


class PostgresStorage:
    def __init__(self, settings: Settings):
        self.settings = settings

    def connect(self):
        import psycopg

        return psycopg.connect(
            host=self.settings.postgres_host, port=self.settings.postgres_port,
            dbname=self.settings.postgres_db, user=self.settings.postgres_user,
            password=self.settings.postgres_password, connect_timeout=10,
        )

    def replace_gold(self, rows: list[dict], quality: dict):
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS gold.ventas_diarias (
                        fecha DATE NOT NULL, ciudad TEXT NOT NULL, producto TEXT NOT NULL,
                        ventas BIGINT NOT NULL, unidades BIGINT NOT NULL, ingresos NUMERIC(18,2) NOT NULL,
                        PRIMARY KEY (fecha, ciudad, producto)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS gold.calidad (
                        archivo_bronze TEXT NOT NULL, filas_bronze INTEGER NOT NULL,
                        filas_silver INTEGER NOT NULL, filas_rechazadas INTEGER NOT NULL,
                        generado_utc TIMESTAMPTZ NOT NULL
                    )
                """)
                cursor.execute("TRUNCATE gold.ventas_diarias, gold.calidad")
                cursor.executemany(
                    "INSERT INTO gold.ventas_diarias VALUES (%s, %s, %s, %s, %s, %s)",
                    [(r["fecha"], r["ciudad"], r["producto"], r["ventas"], r["unidades"], r["ingresos"]) for r in rows],
                )
                cursor.execute("INSERT INTO gold.calidad VALUES (%s, %s, %s, %s, %s)", (
                    quality["archivo_bronze"], quality["filas_bronze"], quality["filas_silver"],
                    quality["filas_rechazadas"], quality["generado_utc"],
                ))

