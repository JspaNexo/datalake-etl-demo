import unittest
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch

from datalake_demo.config.settings import Settings
import hashlib
from datalake_demo.infra.postgres_source import PostgresSalesSource


class PostgresSourceTests(unittest.TestCase):
    def test_snapshot_preserves_types_and_has_stable_identity(self):
        source = SimpleNamespace(read_sales=lambda: [
            (101, date(2026, 10, 7), 'La Paz', 'Café, especial', 2, Decimal('25.50')),
        ])
        first = PostgresSalesSource.read_snapshot(source)
        second = PostgresSalesSource.read_snapshot(source)
        self.assertEqual(first.payload, second.payload)
        self.assertEqual(first.sha256, second.sha256)
        self.assertEqual(first.rows[0]['producto'], 'Café, especial')
        self.assertEqual(first.rows[0]['fecha'], '2026-10-07')
        self.assertEqual(first.rows[0]['precio_unitario'], '25.50')
        self.assertEqual(first.sha256, hashlib.sha256(first.payload).hexdigest())

    def test_changed_database_snapshot_changes_identity(self):
        source = Mock()
        source.read_sales.side_effect = [
            [(1, date(2026, 10, 7), 'La Paz', 'Cafe', 1, Decimal('25.00'))],
            [(1, date(2026, 10, 7), 'La Paz', 'Cafe', 2, Decimal('25.00'))],
        ]
        self.assertNotEqual(PostgresSalesSource.read_snapshot(source).sha256, PostgresSalesSource.read_snapshot(source).sha256)

    def test_origin_connection_does_not_use_gold_credentials(self):
        settings = Settings(source_postgres_host='source', source_postgres_port=5433,
                            source_postgres_db='operacion', source_postgres_user='reader',
                            source_postgres_password='reader-secret', postgres_host='gold')
        connect = Mock()
        with patch.dict('sys.modules', {'psycopg': SimpleNamespace(connect=connect)}):
            PostgresSalesSource(settings).connect()
        connect.assert_called_once_with(host='source', port=5433, dbname='operacion',
                                        user='reader', password='reader-secret', connect_timeout=10)
        self.assertNotIn('reader-secret', repr(settings))

    def test_database_profile_uses_separate_outputs(self):
        csv = Settings()
        database = csv.for_database_source()
        self.assertEqual(csv.dataset, 'ventas')
        self.assertEqual(database.dataset, 'ventas_db')
        for attribute in ('gold_table', 'quality_table'):
            self.assertNotEqual(getattr(csv, attribute), getattr(database, attribute))
        with self.assertRaises(ValueError):
            Settings(dataset='ventas_db')
