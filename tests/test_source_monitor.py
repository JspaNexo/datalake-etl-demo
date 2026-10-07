"""Verifica cambios de contenido, estabilidad y recuperacion del estado del sensor."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from datalake_demo.jobs.source_monitor import evaluate_source_change, evaluate_content_change


class SourceMonitorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.source = Path(self.directory.name) / "ventas.csv"
        self.source.write_bytes(b"ventas A\n")

    def submit(self, cursor=None):
        first = evaluate_source_change(self.source, cursor)
        self.assertIsNone(first.run_key)
        return evaluate_source_change(self.source, first.cursor)

    def test_first_content_requires_two_stable_readings(self):
        request = self.submit()
        self.assertEqual(request.sha256, hashlib.sha256(self.source.read_bytes()).hexdigest())
        self.assertIsNotNone(request.run_key)

    def test_unchanged_content_after_restart_or_touch_is_skipped(self):
        request = self.submit()
        self.source.touch()
        unchanged = evaluate_source_change(self.source, request.cursor)
        self.assertIsNone(unchanged.run_key)
        self.assertEqual(unchanged.cursor, request.cursor)

    def test_edit_between_readings_requires_stability_again(self):
        first = evaluate_source_change(self.source, None)
        self.source.write_bytes(b"ventas B\n")
        second = evaluate_source_change(self.source, first.cursor)
        self.assertIsNone(second.run_key)
        self.assertEqual(evaluate_source_change(self.source, second.cursor).sha256,
                         hashlib.sha256(b"ventas B\n").hexdigest())

    def test_reverting_to_previous_content_creates_a_new_revision(self):
        initial = self.submit()
        self.source.write_bytes(b"ventas B\n")
        updated = self.submit(initial.cursor)
        self.source.write_bytes(b"ventas A\n")
        reverted = self.submit(updated.cursor)
        self.assertEqual(reverted.sha256, initial.sha256)
        self.assertNotEqual(reverted.run_key, initial.run_key)
        self.assertEqual(json.loads(reverted.cursor)["revision"], 3)

    def test_active_run_does_not_consume_a_change(self):
        previous = self.submit()
        self.source.write_bytes(b"ventas B\n")
        waiting = evaluate_source_change(self.source, previous.cursor, run_in_progress=True)
        self.assertIsNone(waiting.run_key)
        self.assertEqual(json.loads(waiting.cursor)["submitted_sha256"], previous.sha256)
        self.assertIsNotNone(self.submit(waiting.cursor).run_key)

    def test_missing_or_unstable_file_is_skipped_without_consuming_it(self):
        first = evaluate_source_change(self.source, None)
        with patch("datalake_demo.jobs.source_monitor.stable_file_sha256", side_effect=OSError):
            skipped = evaluate_source_change(self.source, first.cursor)
        self.assertNotIn("candidate_sha256", json.loads(skipped.cursor))
        self.assertIsNone(evaluate_source_change(self.source, skipped.cursor).run_key)
        self.source.unlink()
        self.assertIsNone(evaluate_source_change(self.source, None).run_key)


    def test_database_changes_do_not_require_a_quiet_period(self):
        cursor = None
        for digest in ("A", "B", "C", "A"):
            change = evaluate_content_change(digest, cursor, stable_readings=1, run_key_prefix="ventas_db")
            self.assertIsNotNone(change.run_key)
            self.assertEqual(change.sha256, digest)
            cursor = change.cursor
        self.assertIsNone(evaluate_content_change("A", cursor, stable_readings=1).run_key)
