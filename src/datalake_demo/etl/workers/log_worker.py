"""Emite un evento periódico con dependencias inyectadas."""

import logging
from datetime import datetime
from math import isfinite
from threading import Event
from uuid import uuid4

from datalake_demo.etl.ports import Clock


class LogWorker:
    def __init__(self, logger: logging.Logger, clock: Clock, interval_seconds: float = 60.0):
        if not isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError("El intervalo debe ser positivo y finito")
        self.logger = logger
        self.clock = clock
        self.interval_seconds = interval_seconds
        self.worker_id = str(uuid4())

    def _lifecycle(self, status: str) -> dict:
        return {"correlation_id": self.worker_id, "stage": "worker", "properties": {
            "WorkerId": self.worker_id, "ProcessName": self.logger.name, "WorkerStatus": status,
        }}

    def run(self, stop: Event) -> None:
        if stop.is_set():
            return
        cycle = 0
        self.logger.info("Iniciando proceso continuo del worker: %s", self.logger.name,
                         extra=self._lifecycle("started"))
        try:
            while not stop.is_set():
                cycle += 1
                extra = {
                    "correlation_id": str(uuid4()), "iteration": cycle, "stage": "heartbeat",
                    "properties": {
                        "WorkerId": self.worker_id, "ProcessName": self.logger.name,
                        "Cycle": cycle, "IntervalSeconds": self.interval_seconds,
                    },
                }
                try:
                    executed_at = datetime.fromisoformat(self.clock())
                    timestamp = executed_at.isoformat()
                    extra["properties"]["ExecutedAt"] = timestamp
                    message = (
                        f"[{executed_at.strftime('%Y-%m-%d %H:%M:%S')}] "
                        f"{self.logger.name} | Proceso {self.logger.name} ejecutado correctamente... "
                        f"FechaHora: ({timestamp})"
                    )
                    self.logger.info(message, extra=extra)
                except Exception as exc:
                    self.logger.exception("Error en ejecución iteración %s: %s", cycle, exc, extra=extra)
                if stop.wait(self.interval_seconds):
                    break
        finally:
            self.logger.info("Proceso continuo del worker detenido: %s", self.logger.name,
                             extra=self._lifecycle("stopped"))
