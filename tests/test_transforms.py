import unittest
from decimal import Decimal
from pathlib import Path

from datalake_demo.etl.extract import read_sales
from datalake_demo.etl.transform import aggregate_sales, clean_sales

SOURCE = Path(__file__).resolve().parent / "fixtures" / "ventas.csv"


class SalesTests(unittest.TestCase):
    def setUp(self):
        self.raw = read_sales(SOURCE.read_bytes())

    def test_demo_reconciles_rows_and_rejects_bad_data(self):
        valid, rejected = clean_sales(self.raw)
        self.assertEqual((len(self.raw), len(valid), len(rejected)), (14, 8, 6))
        self.assertEqual({r["datos"]["venta_id"] for r in rejected}, {"1", "9", "10", "11", "12", "13"})
        self.assertTrue(all(r["motivo"] for r in rejected))

    def test_normalization_and_exact_money(self):
        valid, _ = clean_sales(self.raw)
        self.assertEqual((valid[0]["ciudad"], valid[0]["producto"]), ("La Paz", "cafe"))
        self.assertEqual(valid[0]["importe"], Decimal("50.00"))
        self.assertEqual(valid[1]["producto"], "te")

    def test_gold_matches_expected_business_totals(self):
        valid, _ = clean_sales(self.raw)
        gold = aggregate_sales(valid)
        self.assertEqual(len(gold), 7)
        self.assertEqual(sum(r["ventas"] for r in gold), 8)
        self.assertEqual(sum(r["unidades"] for r in gold), 22)
        self.assertEqual(sum(r["ingresos"] for r in gold), Decimal("356.00"))
        cities = {city: sum(r["ingresos"] for r in gold if r["ciudad"] == city)
                  for city in ("La Paz", "Cochabamba", "Santa Cruz")}
        self.assertEqual(cities, {"La Paz": Decimal("111"), "Cochabamba": Decimal("115"), "Santa Cruz": Decimal("130")})

    def test_non_finite_prices_and_fractional_quantities_are_rejected(self):
        for column, value in [("precio_unitario", "NaN"), ("precio_unitario", "Infinity"),
                              ("precio_unitario", "0.001"), ("precio_unitario", "1E100"),
                              ("cantidad", "1.5"), ("cantidad", "2147483648"),
                              ("fecha", "2026-02-30")]:
            with self.subTest(column=column, value=value):
                valid, rejected = clean_sales([{**self.raw[0], column: value}])
                self.assertEqual((len(valid), len(rejected)), (0, 1))

    def test_invalid_duplicate_does_not_displace_valid_record(self):
        valid, rejected = clean_sales([{**self.raw[0], "cantidad": "0"}, self.raw[0]])
        self.assertEqual((len(valid), len(rejected)), (1, 1))
        self.assertEqual(valid[0]["venta_id"], 1)

    def test_schema_and_malformed_rows(self):
        with self.assertRaises(ValueError):
            read_sales(b"id,valor\n1,2\n")
        valid, rejected = clean_sales([{**self.raw[0], None: ["extra"]}])
        self.assertEqual((len(valid), len(rejected)), (0, 1))

    def test_empty_input(self):
        self.assertEqual(clean_sales([]), ([], []))
        self.assertEqual(aggregate_sales([]), [])


if __name__ == "__main__":
    unittest.main()
