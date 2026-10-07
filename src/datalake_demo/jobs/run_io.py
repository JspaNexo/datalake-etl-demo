"""Persistencia de referencias por run, también al reejecutar pasos desde Runs."""

import os
import pickle
import tempfile
from pathlib import Path

from dagster import IOManager, io_manager


class RunReferenceIOManager(IOManager):
    def __init__(self, directory: Path):
        self.directory = directory

    def _path(self, output_context):
        # get_identifier incorpora el run de origen al reejecutar un paso.
        # get_asset_identifier omitiría el run y mezclaría ejecuciones concurrentes.
        return self.directory.joinpath(*output_context.get_identifier()).with_suffix(".pickle")

    def handle_output(self, context, obj):
        path = self._path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
                temporary = Path(stream.name)
                pickle.dump(obj, stream, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(temporary, path)
        finally:
            if temporary is not None and temporary.exists():
                temporary.unlink()

    def load_input(self, context):
        if context.upstream_output is None:
            raise ValueError("Selecciona el grupo completo o reejecuta los pasos desde Runs")
        path = self._path(context.upstream_output)
        if not path.is_file():
            raise ValueError("No existe la referencia de este run; selecciona el grupo completo o reejecuta desde Runs")
        with path.open("rb") as stream:
            return pickle.load(stream)


@io_manager
def run_reference_io_manager(context):
    return RunReferenceIOManager(Path(context.instance.storage_directory()) / "etl_references")
