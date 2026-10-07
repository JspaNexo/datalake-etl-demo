"""Verifica el fixture estable de CSV dentro del entorno aislado de CI."""

import json
from pathlib import Path

from datalake_demo.config.settings import Settings
from datalake_demo.jobs.definitions import ventas_csv_sensor
from datalake_demo.jobs.verification import check, run_twice, verify_current, write_summary


def verify():
    settings = Settings.from_env()
    expected = json.loads(Path("/app/tests/fixtures/ventas.expected.json").read_text(encoding="utf-8"))
    runs = run_twice(settings, "etl_ventas", ventas_csv_sensor, 2)
    actual = verify_current(settings)
    check(actual == expected, "resultados coinciden con el fixture estable")
    write_summary("csv-summary.json", {**actual, "runs": runs})


if __name__ == "__main__":
    verify()
