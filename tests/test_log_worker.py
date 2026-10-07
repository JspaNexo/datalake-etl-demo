import logging
import unittest
from threading import Event
from unittest.mock import Mock

from datalake_demo.config.settings import Settings
from datalake_demo.etl.workers.log_worker import LogWorker


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


class LogWorkerTests(unittest.TestCase):
    def setUp(self):
        self.capture = Capture()
        self.logger = logging.Logger("worker_test", level=logging.INFO)
        self.logger.addHandler(self.capture)
        self.clock = lambda: "2026-10-07T16:00:00-04:00"

    def test_cycles_have_distinct_correlations_and_stable_worker_identity(self):
        stop = Mock(spec=Event)
        stop.is_set.return_value = False
        stop.wait.side_effect = [False, True]
        LogWorker(self.logger, self.clock).run(stop)

        heartbeat = [r for r in self.capture.records if r.stage == "heartbeat"]
        first, second = heartbeat
        self.assertEqual(first.getMessage(),
                         "[2026-10-07 16:00:00] worker_test | Proceso worker_test ejecutado correctamente... "
                         "FechaHora: (2026-10-07T16:00:00-04:00)")
        self.assertEqual(first.stage, "heartbeat")
        self.assertNotEqual(first.correlation_id, second.correlation_id)
        self.assertEqual(first.properties["WorkerId"], second.properties["WorkerId"])
        self.assertEqual([r.properties["Cycle"] for r in heartbeat], [1, 2])
        self.assertEqual([r.iteration for r in heartbeat], [1, 2])
        self.assertEqual(first.properties["IntervalSeconds"], 60.0)
        self.assertEqual([call.args[0] for call in stop.wait.call_args_list], [60.0, 60.0])
        lifecycle = [r.properties for r in self.capture.records if r.stage == "worker"]
        self.assertEqual([p["WorkerStatus"] for p in lifecycle], ["started", "stopped"])
        self.assertTrue(all(p["WorkerId"] == first.properties["WorkerId"] for p in lifecycle))

    def test_stopped_worker_does_not_emit_another_cycle(self):
        stop = Event()
        stop.set()
        LogWorker(self.logger, self.clock).run(stop)
        self.assertEqual(self.capture.records, [])

    def test_invalid_intervals_are_rejected_before_starting(self):
        for interval in (0, -1, float("nan"), float("inf")):
            with self.subTest(interval=interval):
                with self.assertRaises(ValueError):
                    LogWorker(self.logger, self.clock, interval)
                with self.assertRaises(ValueError):
                    Settings(worker_interval_seconds=interval)

    def test_failed_iteration_is_logged_and_next_cycle_still_runs(self):
        clock = Mock(side_effect=[ValueError("Fallo de captura"), self.clock()])
        stop = Mock(spec=Event)
        stop.is_set.return_value = False
        stop.wait.side_effect = [False, True]
        LogWorker(self.logger, clock).run(stop)

        failed, succeeded = [r for r in self.capture.records if r.stage == "heartbeat"]
        self.assertEqual(failed.levelno, logging.ERROR)
        self.assertIsNotNone(failed.exc_info)
        self.assertEqual(failed.iteration, 1)
        self.assertEqual(succeeded.levelno, logging.INFO)
        self.assertEqual(succeeded.iteration, 2)
