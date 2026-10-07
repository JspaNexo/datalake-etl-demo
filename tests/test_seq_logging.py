import io
import json
import logging
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from threading import Event
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from datalake_demo.config.logging import SeqHttpHandler, get_logger
from datalake_demo.config.settings import Settings
from datalake_demo.etl.workers.log_worker import LogWorker


class SeqLoggingTests(unittest.TestCase):
    def test_http_event_uses_clef_contract_and_structured_properties(self):
        handler = SeqHttpHandler(Settings(seq_url="http://seq:5341/", seq_api_key="test-key"))
        record = logging.LogRecord("ventas", logging.INFO, __file__, 1, "Silver: %s ventas", (8,), None)
        record.correlation_id = "run-test"
        record.stage = "silver"
        record.iteration = 8
        record.properties = {"ValidRows": 8, "RejectedRows": 6, "ExecutionId": "execution-1",
                             "Revision": 3, "SourceType": "csv", "Dataset": "ventas"}
        with patch("datalake_demo.config.logging.urlopen") as send:
            handler.emit(record)
        request = send.call_args.args[0]
        self.assertEqual(request.full_url, "http://seq:5341/api/events/raw")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Content-type"), "application/vnd.serilog.clef")
        self.assertEqual(request.get_header("X-seq-apikey"), "test-key")
        self.assertTrue(request.data.endswith(b"\n"))
        event = json.loads(request.data)
        self.assertEqual(event["@m"], "Silver: 8 ventas")
        self.assertEqual(event["@l"], "Information")
        self.assertTrue(event["@t"].endswith("+00:00"))
        self.assertEqual((event["Application"], event["Environment"], event["Logger"]),
                         ("datalake_demo", "demo", "ventas"))
        self.assertEqual((event["CorrelationId"], event["Stage"]), ("run-test", "silver"))
        self.assertEqual((event["ValidRows"], event["RejectedRows"]), (8, 6))
        self.assertEqual(event["Iteration"], 8)
        self.assertEqual((event["ExecutionId"], event["Revision"], event["SourceType"], event["Dataset"]),
                         ("execution-1", 3, "csv", "ventas"))

    def test_exception_detail_and_optional_authentication(self):
        try:
            raise ValueError("cantidad invalida")
        except ValueError:
            record = logging.LogRecord("ventas", logging.ERROR, __file__, 1, "Error silver", (), sys.exc_info())
        with patch("datalake_demo.config.logging.urlopen") as send:
            SeqHttpHandler(Settings()).emit(record)
        request = send.call_args.args[0]
        self.assertIsNone(request.get_header("X-seq-apikey"))
        event = json.loads(request.data)
        self.assertEqual(event["@l"], "Error")
        self.assertIn("ValueError: cantidad invalida", event["@x"])

    def test_seq_http_failure_does_not_interrupt_or_lose_file_log(self):
        with tempfile.TemporaryDirectory() as temp_dir, redirect_stderr(io.StringIO()):
            settings = Settings(app_name="seq_failure_test", log_dir=Path(temp_dir))
            logger = get_logger(settings)
            seq_handler = next(handler for handler in logger.handlers if isinstance(handler, SeqHttpHandler))
            try:
                error = HTTPError("http://seq:5341", 503, "Unavailable", {}, None)
                with patch("datalake_demo.config.logging.urlopen", side_effect=error), patch.object(seq_handler, "handleError") as report:
                    logger.info("Gold completado")
                report.assert_called_once()
                self.assertIn("Gold completado", settings.log_file_path.read_text(encoding="utf-8"))
            finally:
                for handler in list(logger.handlers):
                    logger.removeHandler(handler)
                    handler.close()

    def test_worker_sends_the_same_cycle_message_to_txt_console_and_seq(self):
        with tempfile.TemporaryDirectory() as temp_dir, redirect_stderr(io.StringIO()) as console:
            settings = Settings(app_name="original_worker_test", log_dir=Path(temp_dir),
                                log_file_name="worker_log.txt")
            logger = get_logger(settings)
            stop = Mock(spec=Event)
            stop.is_set.return_value = False
            stop.wait.return_value = True
            try:
                with patch("datalake_demo.config.logging.urlopen") as send:
                    LogWorker(logger, lambda: "2026-10-07T16:00:00-04:00").run(stop)
                events = [json.loads(call.args[0].data) for call in send.call_args_list]
                self.assertEqual([e["Stage"] for e in events], ["worker", "heartbeat", "worker"])
                cycle = events[1]
                self.assertEqual(cycle["Iteration"], 1)
                self.assertEqual(cycle["Cycle"], 1)
                self.assertEqual(cycle["ExecutedAt"], "2026-10-07T16:00:00-04:00")
                self.assertIn(cycle["@m"], settings.log_file_path.read_text(encoding="utf-8"))
                self.assertIn(cycle["@m"], console.getvalue())
                self.assertEqual(events[0]["WorkerStatus"], "started")
                self.assertEqual(events[-1]["WorkerStatus"], "stopped")
            finally:
                for handler in list(logger.handlers):
                    logger.removeHandler(handler)
                    handler.close()


if __name__ == "__main__":
    unittest.main()
