"""Pruebas con servicios reales; ejecutar solo dentro de compose.ci.yaml."""

import logging
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

from datalake_demo.bootstrap import build_pipeline
from datalake_demo.config.settings import Settings
from datalake_demo.infra.csv_source import snapshot_from_bytes
from datalake_demo.jobs.verification import verify_current

HEADER = b"venta_id,fecha,ciudad,producto,cantidad,precio_unitario\n"


class StorageIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("APP_ENV") != "ci":
            raise unittest.SkipTest("Estas pruebas escriben solamente en servicios de CI")
        cls.settings = Settings.from_env()
        cls.logger = logging.getLogger("storage_integration")
        cls.logger.handlers = [logging.NullHandler()]
        cls.logger.propagate = False

    def pipeline(self, price="10.00", database=False):
        settings = self.settings.for_database_source() if database else self.settings
        pipeline = build_pipeline(settings, logger=self.logger)
        payload = HEADER if price is None else HEADER + f"1,2026-10-07,La Paz,cafe,1,{price}\n".encode()
        pipeline.source = SimpleNamespace(read_snapshot=lambda: snapshot_from_bytes(payload))
        return pipeline

    def prepare(self, pipeline):
        bronze = pipeline.bronze()
        return pipeline.gold(pipeline.silver(bronze))

    def test_interleaving_and_older_run_are_safe_with_real_objects(self):
        a, b = self.pipeline(), self.pipeline("99.00")
        sa = a.silver(a.bronze())
        sb = b.silver(b.bronze())
        ga, gb = a.gold(sa), b.gold(sb)
        a.publish_gold(ga)
        self.assertEqual(verify_current(self.settings)["revenue_bs"], "10.00")
        b.publish_gold(gb)
        self.assertEqual(a.publish_gold(ga).status, "superseded")
        self.assertEqual(verify_current(self.settings)["revenue_bs"], "99.00")
        self.assertEqual(a.lake.read_silver(sa)[0]["importe"].to_eng_string(), "10.00")

    def test_concurrent_publishers_keep_the_highest_revision(self):
        pipelines = [self.pipeline(str(n) + ".00") for n in range(1, 5)]
        results = [self.prepare(p) for p in pipelines]
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(p.publish_gold, g) for p, g in zip(pipelines, results)]
            for future in futures:
                future.result(timeout=30)
        current = pipelines[0].publications.current("ventas")
        self.assertEqual(current.revision, max(g.execution.revision for g in results))
        self.assertEqual(verify_current(self.settings)["revenue_bs"], "4.00")

    def test_each_object_failure_keeps_current_and_retry_completes(self):
        self.pipeline().run()
        for suffix in ("ventas_limpias.parquet", "rechazados.json", "calidad.json", "ventas_diarias.parquet", "manifest.json"):
            with self.subTest(suffix=suffix):
                pipeline = self.pipeline("99.00")
                bronze = pipeline.bronze()
                before = pipeline.publications.current("ventas")
                original = pipeline.lake._put
                def fail(key, payload, content_type):
                    if key.endswith(suffix):
                        raise OSError("Fallo de escritura simulado")
                    return original(key, payload, content_type)
                with patch.object(pipeline.lake, "_put", side_effect=fail):
                    with self.assertRaises(OSError):
                        pipeline.publish_gold(pipeline.gold(pipeline.silver(bronze)))
                self.assertEqual(pipeline.publications.current("ventas"), before)
                result = pipeline.publish_gold(pipeline.gold(pipeline.silver(bronze)))
                self.assertEqual(result.status, "published")
                self.assertEqual(verify_current(self.settings)["revenue_bs"], "99.00")

    def test_commit_response_loss_is_reconciled_without_republishing(self):
        import psycopg
        pipeline = self.pipeline("25.00")
        gold = self.prepare(pipeline)
        original = pipeline.publications._publish_once
        def commit_then_disconnect(*args):
            original(*args)
            raise psycopg.OperationalError("Respuesta COMMIT perdida")
        with patch.object(pipeline.publications, "_publish_once", side_effect=commit_then_disconnect):
            self.assertEqual(pipeline.publish_gold(gold).status, "already_published")
        current = pipeline.publications.current("ventas")
        self.assertEqual(pipeline.publish_gold(gold).status, "already_published")
        self.assertEqual(pipeline.publications.current("ventas"), current)
        self.assertEqual(verify_current(self.settings)["revenue_bs"], "25.00")

    def test_sql_error_rolls_back_gold_quality_and_pointer(self):
        from decimal import Decimal
        import psycopg
        pipeline = self.pipeline()
        pipeline.run()
        before = pipeline.publications.current("ventas")
        gold = self.prepare(self.pipeline("99.00"))
        prepared = pipeline.lake.prepare(gold)
        rows, quality = pipeline.lake.read_publication(prepared)
        rows[0]["ingresos"] = Decimal("1E30")
        with self.assertRaises(psycopg.Error):
            pipeline.publications.publish(prepared, rows, quality)
        self.assertEqual(pipeline.publications.current("ventas"), before)
        self.assertEqual(verify_current(self.settings)["revenue_bs"], "10.00")

    def test_empty_snapshots_clear_both_profiles_and_keep_schemas(self):
        for database in (False, True):
            pipeline = self.pipeline(database=database)
            pipeline.run()
            empty = self.pipeline(None, database)
            result = empty.run()
            stats = verify_current(self.settings.for_database_source() if database else self.settings)
            self.assertEqual((stats["valid_rows"], stats["groups"], stats["revenue_bs"]), (0, 0, "0.00"))
            self.assertEqual(empty.lake.read_silver(result["silver"]), [])

    def test_immutable_object_and_manifest_hash_checks(self):
        pipeline = self.pipeline()
        result = pipeline.run()
        prepared = pipeline.lake.read_prepared(result["sql"].manifest)
        with self.assertRaises(ValueError):
            pipeline.lake._put(result["gold"].artifact.key, b"changed", "application/octet-stream")
        from dataclasses import replace
        with self.assertRaises(ValueError):
            pipeline.lake.read_prepared(replace(prepared.manifest, sha256="wrong"))
