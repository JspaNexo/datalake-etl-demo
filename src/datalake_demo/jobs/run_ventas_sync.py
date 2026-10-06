"""Punto de entrada por consola: configura la aplicacion y ejecuta un ciclo."""

import argparse
import json

from datalake_demo.config.settings import Settings
from datalake_demo.etl.pipelines.ventas_pipeline import SalesPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Ejecutar ETL de ventas: bronze -> silver -> gold -> SQL")
    parser.add_argument("--source", help="CSV de entrada; por defecto se usa SOURCE_PATH")
    args = parser.parse_args()
    pipeline = SalesPipeline.from_settings(Settings.from_env())
    print(json.dumps(pipeline.run(args.source), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

