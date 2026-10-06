import json
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from urllib.request import Request, urlopen

from datalake_demo.config.settings import Settings
from datalake_demo.infra.file_storage import ensure_directory


class SeqHttpHandler(logging.Handler):
    """Envia eventos CLEF a Seq, como el handler de pyworkerlogsdemo."""

    LEVELS = {
        logging.NOTSET: "Verbose", logging.DEBUG: "Debug", logging.INFO: "Information",
        logging.WARNING: "Warning", logging.ERROR: "Error", logging.CRITICAL: "Fatal",
    }

    def __init__(self, settings: Settings):
        super().__init__()
        self.settings = settings
        self.ingest_url = f"{settings.seq_url.rstrip('/')}/api/events/raw"

    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload = self._build_clef_event(record)
            headers = {"Content-Type": "application/vnd.serilog.clef"}
            if self.settings.seq_api_key:
                headers["X-Seq-ApiKey"] = self.settings.seq_api_key
            request = Request(
                self.ingest_url, method="POST", headers=headers,
                data=(json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"),
            )
            with urlopen(request, timeout=self.settings.seq_timeout_seconds):
                pass
        except Exception:
            # El fallo de Seq no interrumpe el ETL; consola y archivo ya recibieron el evento.
            self.handleError(record)

    def _build_clef_event(self, record: logging.LogRecord) -> dict:
        event = dict(getattr(record, "properties", {}))
        event.update({
            "@t": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "@m": record.getMessage(),
            "@l": self.LEVELS.get(record.levelno, "Information"),
            "Application": self.settings.app_name,
            "Environment": self.settings.app_env,
            "Logger": record.name,
        })
        for attribute, property_name in (
            ("correlation_id", "CorrelationId"), ("iteration", "Iteration"), ("stage", "Stage"),
        ):
            value = getattr(record, attribute, None)
            if value is not None:
                event[property_name] = value
        if record.exc_info:
            event["@x"] = logging.Formatter().formatException(record.exc_info)
        return event


def get_logger(settings: Settings) -> logging.Logger:
    logger = logging.getLogger(settings.app_name)
    logger.setLevel(settings.log_level)
    logger.propagate = False
    if logger.handlers:
        return logger

    ensure_directory(settings.log_dir)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    file_handler = RotatingFileHandler(
        settings.log_file_path, maxBytes=1_000_000, backupCount=2, encoding="utf-8",
    )
    console_handler = logging.StreamHandler()
    seq_handler = SeqHttpHandler(settings)
    for handler in (file_handler, console_handler, seq_handler):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger