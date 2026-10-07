import logging
import unittest
from pathlib import Path

from datalake_demo.etl.pipelines.ventas_pipeline import SalesPipeline
from datalake_demo.infra.csv_source import CsvSalesSource, snapshot_from_bytes
from fakes import MemoryLake, MemoryPublications

HEADER = b"venta_id,fecha,ciudad,producto,cantidad,precio_unitario\n"


class Source:
    def __init__(self, price="10.00"):
        self.payload = HEADER + f"1,2026-10-07,La Paz,cafe,1,{price}\n".encode()

    def read_snapshot(self):
        return snapshot_from_bytes(self.payload)


class VersionedPipelineTests(unittest.TestCase):
    def setUp(self):
        self.lake, self.publications = MemoryLake(), MemoryPublications()
        self.logger = logging.getLogger("versioned_test")
        self.logger.handlers = [logging.NullHandler()]
        self.logger.propagate = False

    def pipeline(self, source=None, dataset="ventas"):
        return SalesPipeline(dataset, "csv" if dataset == "ventas" else "postgres", source or Source(),
                             self.lake, self.publications, lambda: "2026-10-07T12:00:00+00:00", self.logger)

    def test_interleaved_runs_keep_their_own_source_quality_and_totals(self):
        a, b = self.pipeline(Source("10.00")), self.pipeline(Source("99.00"))
        sa = a.silver(a.bronze())
        sb = b.silver(b.bronze())
        ga, gb = a.gold(sa), b.gold(sb)
        a.publish_gold(ga)
        self.assertEqual(str(self.publications.rows["ventas"][0]["ingresos"]), "10.00")
        self.assertEqual(self.publications.quality["ventas"]["archivo_bronze"], sa.bronze.artifact.key)
        b.publish_gold(gb)
        self.assertEqual(str(self.publications.rows["ventas"][0]["ingresos"]), "99.00")

    def test_older_run_finishing_later_cannot_replace_newer_publication(self):
        a, b = self.pipeline(), self.pipeline(Source("99.00"))
        ga = a.gold(a.silver(a.bronze()))
        b.run()
        current = self.publications.current("ventas")
        self.assertEqual(a.publish_gold(ga).status, "superseded")
        self.assertEqual(self.publications.current("ventas"), current)

    def test_failures_at_each_write_keep_previous_publication(self):
        self.pipeline().run()
        current = self.publications.current("ventas")
        for stage in ("bronze", "silver", "rejected", "quality", "gold", "manifest"):
            with self.subTest(stage=stage):
                self.lake.fail_at = stage
                with self.assertRaises(OSError):
                    self.pipeline(Source("99.00")).run()
                self.assertEqual(self.publications.current("ventas"), current)
                self.assertEqual(str(self.publications.rows["ventas"][0]["ingresos"]), "10.00")
        self.lake.fail_at = None
        self.publications.fail = True
        with self.assertRaises(OSError):
            self.pipeline(Source("99.00")).run()
        self.assertEqual(self.publications.current("ventas"), current)

    def test_retry_reuses_captured_source_even_if_input_changes(self):
        source = Source()
        pipeline = self.pipeline(source)
        bronze = pipeline.bronze()
        source.payload = Source("99.00").payload
        self.assertEqual(pipeline.bronze(), bronze)
        gold = pipeline.gold(pipeline.silver(bronze))
        self.publications.fail = True
        with self.assertRaises(OSError):
            pipeline.publish_gold(gold)
        self.publications.fail = False
        pipeline.publish_gold(gold)
        self.assertEqual(pipeline.publish_gold(gold).status, "already_published")
        self.assertEqual(self.publications.commits, 1)
        self.assertEqual(str(self.publications.rows["ventas"][0]["ingresos"]), "10.00")

    def test_empty_source_publishes_zero_for_both_datasets(self):
        for dataset in ("ventas", "ventas_db"):
            self.pipeline(dataset=dataset).run()
            empty = Source()
            empty.payload = HEADER
            self.pipeline(empty, dataset).run()
            self.assertEqual(self.publications.rows[dataset], [])
            self.assertEqual(self.publications.quality[dataset]["filas_bronze"], 0)

    def test_invalid_or_unavailable_source_keeps_publication(self):
        self.pipeline().run()
        current = self.publications.current("ventas")
        for payload in (b"", b"id,precio\n1,2\n", HEADER + b"1,2026-10-07,La Paz,cafe,0,25.00\n"):
            source = Source()
            source.payload = payload
            with self.assertRaises(ValueError):
                self.pipeline(source).run()
            self.assertEqual(self.publications.current("ventas"), current)
        with self.assertRaises(FileNotFoundError):
            self.pipeline(CsvSalesSource(Path("missing-sales-audit.csv"))).run()

    def test_profiles_are_independent_and_full_runs_have_distinct_ids(self):
        pipeline = self.pipeline()
        first, second = pipeline.run(), pipeline.run()
        self.assertNotEqual(first["sql"].execution_id, second["sql"].execution_id)
        csv = self.publications.current("ventas")
        self.pipeline(Source("99.00"), "ventas_db").run()
        self.assertEqual(self.publications.current("ventas"), csv)

    def test_csv_hash_guard_runs_before_writing_bronze(self):
        with self.assertRaisesRegex(ValueError, "cambio"):
            self.pipeline().bronze(expected_sha256="different")
        self.assertEqual(self.lake.objects, {})
