import os
import tempfile
import unittest
from pathlib import Path

from dagster import AssetKey, build_input_context, build_output_context

from datalake_demo.jobs.run_io import RunReferenceIOManager


class DagsterReferenceTests(unittest.TestCase):
    def test_same_asset_outputs_are_isolated_and_parent_run_can_be_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = RunReferenceIOManager(Path(directory))
            outputs = [build_output_context(run_id=run, step_key="silver_ventas", name="result",
                                             asset_key=AssetKey("silver_ventas")) for run in ("a", "b")]
            manager.handle_output(outputs[0], {"execution_id": "a", "revenue": "10.00"})
            manager.handle_output(outputs[1], {"execution_id": "b", "revenue": "99.00"})
            self.assertEqual(manager.load_input(build_input_context(upstream_output=outputs[0]))["revenue"], "10.00")
            self.assertEqual(manager.load_input(build_input_context(upstream_output=outputs[1]))["revenue"], "99.00")
            reloaded = RunReferenceIOManager(Path(directory))
            self.assertEqual(reloaded.load_input(build_input_context(upstream_output=outputs[0]))["execution_id"], "a")

    @unittest.skipUnless(os.getenv("APP_ENV") == "ci", "Reejecucion nativa escribe solo en CI")
    def test_native_partial_reexecution_uses_parent_snapshot(self):
        from unittest.mock import patch
        from dagster import DagsterInstance, ReexecutionOptions, execute_job, reconstructable
        from datalake_demo.config.settings import Settings
        from datalake_demo.infra.postgres_storage import PostgresStorage
        from scripts.ci.reexecution_job import csv_job

        job = reconstructable(csv_job)
        with patch.dict(os.environ, {"APP_NAME": "datalake_reexecution_test"}), DagsterInstance.local_temp() as instance:
            with execute_job(job, instance=instance) as first:
                self.assertTrue(first.success)
                original_id = first.run_id
            with execute_job(job, instance=instance) as second:
                self.assertTrue(second.success)
            store = PostgresStorage(Settings.from_env())
            current = store.current("ventas")
            options = ReexecutionOptions(parent_run_id=original_id, step_selection=["gold_ventas", "publicar_gold_sql"])
            with execute_job(job, instance=instance, reexecution_options=options) as retry:
                self.assertTrue(retry.success)
                self.assertEqual(retry.output_for_node("gold_ventas").execution.execution_id, original_id)
                self.assertEqual(retry.output_for_node("publicar_gold_sql").status, "superseded")
            self.assertEqual(store.current("ventas"), current)
