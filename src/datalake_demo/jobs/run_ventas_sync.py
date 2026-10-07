"""Punto de entrada de consola."""

import argparse
import json
from dataclasses import asdict, replace
from pathlib import Path

from datalake_demo.bootstrap import build_pipeline
from datalake_demo.config.settings import Settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Ejecutar ETL de ventas: bronze -> silver -> gold -> SQL")
    parser.add_argument("--source", help="CSV de entrada; por defecto SOURCE_PATH")
    parser.add_argument("--source-type", choices=("csv", "postgres"), default="csv")
    args = parser.parse_args()
    if args.source_type == "postgres" and args.source:
        parser.error("--source es una ruta CSV; no se combina con --source-type postgres")
    settings = Settings.from_env()
    if args.source_type == "postgres":
        settings = settings.for_database_source()
    elif args.source:
        settings = replace(settings, source_path=Path(args.source))
    result = build_pipeline(settings).run()
    print(json.dumps({key: asdict(value) for key, value in result.items()}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
