import io
import json
import logging
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from datalake_demo.config.logging import SeqHttpHandler, get_logger
from datalake_demo.config.settings import Settings


class SeqLoggingTests(unittest.TestCase):
    def test_http_event_uses_clef_contract_and_structured_properties(self):
        handler = SeqHttpHandler(Settings(seq_url="http://seq:5341/", seq_api_key="test-key"))
        record = logging.LogRecord("ventas", logging.INFO, __file__, 1, "Silver: %s ventas", (8,), None)
        record.correlation_id = "run-test"
        record.iteration = 1
        record.stage = "silver"
        record.properties = {"ValidRows": 8, "RejectedRows": 6}
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
        self.assertEqual((event["CorrelationId"], event["Stage"], event["Iteration"]), ("run-test", "silver", 1))
        self.assertEqual((event["ValidRows"], event["RejectedRows"]), (8, 6))

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


if __name__ == "__main__":
    unittest.main()
