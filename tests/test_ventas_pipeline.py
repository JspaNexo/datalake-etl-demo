"""Pruebas del flujo completo con adaptadores en memoria, sin servicios externos."""

import copy
import logging
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from datalake_demo.config.settings import GOLD_KEY, QUALITY_KEY, REJECTED_KEY, SILVER_KEY, Settings
from datalake_demo.etl.pipelines.ventas_pipeline import SalesPipeline

SOURCE = Path(__file__).resolve().parent / "fixtures" / "ventas.csv"


class MemoryLake:
    def __init__(self):
        self.originals = {}
        self.parquet = {}
        self.json = {}

    def ensure_bucket(self):
        pass

    def put_original(self, key, payload):
        self.originals.setdefault(key, payload)

    def get_bytes(self, key):
        return self.originals[key]

    def put_parquet(self, key, rows, layer):
        self.parquet[key] = copy.deepcopy(rows)

    def get_parquet(self, key):
        return copy.deepcopy(self.parquet[key])

    def put_json(self, key, value):
        self.json[key] = copy.deepcopy(value)

    def get_json(self, key):
        return copy.deepcopy(self.json[key])


class MemoryWarehouse:
    def __init__(self):
        self.rows = []
        self.quality = {}

    def replace_gold(self, rows, quality):
        self.rows = copy.deepcopy(rows)
        self.quality = copy.deepcopy(quality)


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.lake = MemoryLake()
        self.warehouse = MemoryWarehouse()
        logger = logging.getLogger("pipeline_test")
        logger.addHandler(logging.NullHandler())
        logger.propagate = False
        self.pipeline = SalesPipeline(Settings(source_path=SOURCE), self.lake, self.warehouse, logger)

    def test_complete_pipeline_reconciles_source_layers_and_sql(self):
        result = self.pipeline.run()
        self.assertEqual(self.lake.originals[result["bronze"]["key"]], SOURCE.read_bytes())
        self.assertEqual(len(self.lake.parquet[SILVER_KEY]), 8)
        self.assertEqual(len(self.lake.json[REJECTED_KEY]), 6)
        self.assertEqual(self.warehouse.rows, self.lake.parquet[GOLD_KEY])
        self.assertEqual(self.warehouse.quality, self.lake.json[QUALITY_KEY])
        self.assertEqual(result["gold"]["ingresos_bs"], "356.00")

    def test_repeating_pipeline_does_not_duplicate_sales(self):
        self.pipeline.run()
        self.pipeline.run()
        self.assertEqual(len(self.lake.originals), 1)
        self.assertEqual(len(self.warehouse.rows), 7)
        self.assertEqual(sum(row["ventas"] for row in self.warehouse.rows), 8)
        self.assertEqual(sum(row["ingresos"] for row in self.warehouse.rows), Decimal("356.00"))

    def test_invalid_source_keeps_previous_clean_outputs(self):
        self.pipeline.run()
        previous_parquet = copy.deepcopy(self.lake.parquet)
        previous_quality = copy.deepcopy(self.warehouse.quality)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "invalid.csv"
            path.write_text(
                "venta_id,fecha,ciudad,producto,cantidad,precio_unitario\n"
                "1,2026-10-01,La Paz,cafe,0,25.00\n", encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "No hay ventas validas"):
                self.pipeline.run(path)
        self.assertEqual(self.lake.parquet, previous_parquet)
        self.assertEqual(self.warehouse.quality, previous_quality)
        self.assertEqual(self.warehouse.rows, previous_parquet[GOLD_KEY])

    def test_source_changed_after_detection_is_not_published(self):
        with self.assertRaisesRegex(ValueError, "CSV cambio"):
            self.pipeline.bronze(expected_sha256="a-different-content-hash")
        self.assertEqual(self.lake.originals, {})
        self.assertEqual(self.lake.parquet, {})
        self.assertEqual(self.warehouse.rows, [])


if __name__ == "__main__":
    unittest.main()
