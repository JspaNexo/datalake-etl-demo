"""Registro de ejecuciones y publicación atómica de Gold, calidad y versión vigente."""

from dataclasses import asdict

from datalake_demo.config.settings import Settings
from datalake_demo.etl.models import (
    Artifact, CurrentPublication, Execution, PublicationResult, bronze_from_dict,
)


class PostgresStorage:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._ready = False

    def connect(self):
        import psycopg
        return psycopg.connect(
            host=self.settings.postgres_host, port=self.settings.postgres_port,
            dbname=self.settings.postgres_db, user=self.settings.postgres_user,
            password=self.settings.postgres_password, connect_timeout=10,
        )

    def ensure_schema(self):
        if self._ready:
            return
        from psycopg import sql
        with self.connect() as connection, connection.cursor() as cursor:
            # Serializa únicamente la migración aditiva entre procesos de arranque.
            cursor.execute("SELECT pg_advisory_xact_lock(7040100701)")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS etl_control")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etl_control.ejecuciones (
                    dataset TEXT NOT NULL, execution_id TEXT NOT NULL,
                    revision BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
                    source_type TEXT NOT NULL, created_utc TIMESTAMPTZ NOT NULL,
                    bronze JSONB, PRIMARY KEY (dataset, execution_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS etl_control.publicaciones (
                    dataset TEXT PRIMARY KEY, execution_id TEXT,
                    revision BIGINT NOT NULL DEFAULT 0, manifest JSONB,
                    published_utc TIMESTAMPTZ
                )
            """)
            for gold_name, quality_name in (("ventas_diarias", "calidad"), ("ventas_db_diarias", "calidad_db")):
                cursor.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} (
                        fecha DATE NOT NULL, ciudad TEXT NOT NULL, producto TEXT NOT NULL,
                        ventas BIGINT NOT NULL, unidades BIGINT NOT NULL, ingresos NUMERIC(18,2) NOT NULL,
                        PRIMARY KEY (fecha, ciudad, producto)
                    )
                """).format(sql.Identifier("gold", gold_name)))
                cursor.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} (
                        archivo_bronze TEXT NOT NULL, filas_bronze INTEGER NOT NULL,
                        filas_silver INTEGER NOT NULL, filas_rechazadas INTEGER NOT NULL,
                        generado_utc TIMESTAMPTZ NOT NULL
                    )
                """).format(sql.Identifier("gold", quality_name)))
        self._ready = True

    def _check_dataset(self, dataset):
        if dataset != self.settings.dataset:
            raise ValueError("El repositorio pertenece a otro dataset")

    def begin(self, dataset, execution_id, source_type, created_utc):
        self._check_dataset(dataset)
        self.ensure_schema()
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO etl_control.ejecuciones (dataset, execution_id, source_type, created_utc)
                VALUES (%s,%s,%s,%s) ON CONFLICT (dataset, execution_id) DO NOTHING
            """, (dataset, execution_id, source_type, created_utc))
            cursor.execute("""
                SELECT revision, source_type, created_utc FROM etl_control.ejecuciones
                WHERE dataset=%s AND execution_id=%s
            """, (dataset, execution_id))
            revision, stored_type, timestamp = cursor.fetchone()
            if stored_type != source_type:
                raise ValueError("La ejecucion ya pertenece a otro tipo de fuente")
        return Execution(dataset, execution_id, revision, stored_type, timestamp.isoformat())

    def captured(self, execution):
        self._check_dataset(execution.dataset)
        self.ensure_schema()
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT bronze FROM etl_control.ejecuciones WHERE dataset=%s AND execution_id=%s",
                           (execution.dataset, execution.execution_id))
            row = cursor.fetchone()
            return bronze_from_dict(row[0]) if row and row[0] else None

    def bind_bronze(self, bronze):
        from psycopg.types.json import Jsonb
        execution = bronze.execution
        self._check_dataset(execution.dataset)
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute("""
                UPDATE etl_control.ejecuciones SET bronze=COALESCE(bronze,%s)
                WHERE dataset=%s AND execution_id=%s RETURNING bronze
            """, (Jsonb(asdict(bronze)), execution.dataset, execution.execution_id))
            row = cursor.fetchone()
            if not row:
                raise ValueError("Ejecucion no registrada")
            return bronze_from_dict(row[0])

    def current(self, dataset):
        self._check_dataset(dataset)
        self.ensure_schema()
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute("""
                SELECT execution_id, revision, manifest FROM etl_control.publicaciones
                WHERE dataset=%s AND manifest IS NOT NULL
            """, (dataset,))
            row = cursor.fetchone()
        return CurrentPublication(row[0], row[1], Artifact(**row[2])) if row else None

    @staticmethod
    def _result(prepared, rows, status):
        execution = prepared.gold.execution
        return PublicationResult(execution.execution_id, execution.revision, status, prepared.manifest, len(rows))

    def publish(self, prepared, rows, quality):
        import psycopg
        execution = prepared.gold.execution
        self._check_dataset(execution.dataset)
        self.ensure_schema()
        try:
            return self._publish_once(prepared, rows, quality)
        except psycopg.Error:
            # COMMIT pudo aplicarse aunque su respuesta no llegara al cliente.
            try:
                current = self.current(execution.dataset)
            except psycopg.Error:
                raise
            if current and current.revision > execution.revision:
                return self._result(prepared, rows, "superseded")
            if current and current.execution_id == execution.execution_id and current.manifest == prepared.manifest:
                return self._result(prepared, rows, "already_published")
            raise

    def _publish_once(self, prepared, rows, quality):
        from psycopg import sql
        from psycopg.types.json import Jsonb
        execution = prepared.gold.execution
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.execute("INSERT INTO etl_control.publicaciones (dataset) VALUES (%s) ON CONFLICT DO NOTHING",
                           (execution.dataset,))
            cursor.execute("""
                SELECT execution_id, revision, manifest FROM etl_control.publicaciones
                WHERE dataset=%s FOR UPDATE
            """, (execution.dataset,))
            current_id, revision, manifest = cursor.fetchone()
            if revision > execution.revision:
                return self._result(prepared, rows, "superseded")
            if revision == execution.revision:
                if current_id != execution.execution_id or manifest != asdict(prepared.manifest):
                    raise ValueError("Conflicto de identidad en la publicacion")
                return self._result(prepared, rows, "already_published")
            cursor.execute("SELECT revision, bronze FROM etl_control.ejecuciones WHERE dataset=%s AND execution_id=%s",
                           (execution.dataset, execution.execution_id))
            registered = cursor.fetchone()
            if not registered or registered[0] != execution.revision or registered[1] != asdict(prepared.gold.silver.bronze):
                raise ValueError("La publicacion no corresponde a la captura registrada")
            gold_table = sql.Identifier("gold", self.settings.gold_table)
            quality_table = sql.Identifier("gold", self.settings.quality_table)
            cursor.execute(sql.SQL("TRUNCATE {}, {}").format(gold_table, quality_table))
            cursor.executemany(sql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s,%s,%s)").format(gold_table),
                               [(r["fecha"], r["ciudad"], r["producto"], r["ventas"], r["unidades"], r["ingresos"]) for r in rows])
            cursor.execute(sql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s,%s)").format(quality_table),
                           (quality["archivo_bronze"], quality["filas_bronze"], quality["filas_silver"],
                            quality["filas_rechazadas"], quality["generado_utc"]))
            cursor.execute("""
                UPDATE etl_control.publicaciones
                SET execution_id=%s, revision=%s, manifest=%s, published_utc=CURRENT_TIMESTAMP
                WHERE dataset=%s
            """, (execution.execution_id, execution.revision, Jsonb(asdict(prepared.manifest)), execution.dataset))
        return self._result(prepared, rows, "published")
