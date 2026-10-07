"""Punto de entrada del worker de logs; SIGTERM y Ctrl+C detienen la espera."""

import logging
import signal
from threading import Event

from datalake_demo.bootstrap import build_log_worker
from datalake_demo.config.settings import Settings


def main() -> None:
    worker = build_log_worker(Settings.from_env())
    stop = Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda _signum, _frame: stop.set())
    try:
        worker.run(stop)
    finally:
        logging.shutdown()


if __name__ == "__main__":
    main()
