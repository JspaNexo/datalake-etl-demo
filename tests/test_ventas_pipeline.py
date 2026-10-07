"""Regresión del caso original con contratos independientes de servicios."""

import logging
import unittest
from decimal import Decimal
from pathlib import Path

from datalake_demo.etl.pipelines.ventas_pipeline import SalesPipeline
from datalake_demo.infra.csv_source import CsvSalesSource
from fakes import MemoryLake, MemoryPublications

SOURCE = Path(__file__).resolve().parent / "fixtures" / "ventas.csv"


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.lake, self.publications = MemoryLake(), MemoryPublications()
        logger = logging.getLogger("fixture_test")
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        self.pipeline = SalesPipeline("ventas", "csv", CsvSalesSource(SOURCE), self.lake,
                                      self.publications, lambda: "2026-10-07T12:00:00+00:00", logger)

    def test_complete_pipeline_reconciles_layers_and_publication(self):
        result = self.pipeline.run()
        self.assertEqual(result["bronze"].rows, 14)
        self.assertEqual((result["silver"].rows, result["silver"].rejected_rows), (8, 6))
        self.assertEqual(result["gold"].revenue_bs, "356.00")
        self.assertEqual(self.publications.quality["ventas"]["filas_bronze"], 14)
        self.assertEqual(self.publications.current("ventas").manifest, result["sql"].manifest)

    def test_repeating_pipeline_keeps_totals_and_preserves_history(self):
        first, second = self.pipeline.run(), self.pipeline.run()
        self.assertEqual(first["bronze"].artifact, second["bronze"].artifact)
        self.assertNotEqual(first["gold"].artifact.key, second["gold"].artifact.key)
        self.assertIn(first["gold"].artifact.key, self.lake.objects)
        rows = self.publications.rows["ventas"]
        self.assertEqual((len(rows), sum(r["ventas"] for r in rows)), (7, 8))
        self.assertEqual(sum(r["ingresos"] for r in rows), Decimal("356.00"))
