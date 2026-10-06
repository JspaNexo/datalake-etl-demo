"""Decide cuando procesar el CSV, sin depender de Dagster ni servicios externos."""

import json
from dataclasses import dataclass
from pathlib import Path

from datalake_demo.infra.file_storage import stable_file_sha256


@dataclass(frozen=True)
class SourceChange:
    cursor: str
    reason: str | None = None
    sha256: str | None = None
    run_key: str | None = None


def evaluate_source_change(source: Path, cursor: str | None, *, run_in_progress: bool = False) -> SourceChange:
    state = json.loads(cursor) if cursor else {}

    def skip(reason: str) -> SourceChange:
        return SourceChange(cursor=json.dumps(state, sort_keys=True), reason=reason)

    if run_in_progress:
        state.pop("candidate_sha256", None)
        return skip("Hay una ejecucion de etl_ventas en curso; esperamos a que termine")
    try:
        digest = stable_file_sha256(source)
    except OSError:
        state.pop("candidate_sha256", None)
        return skip(f"El CSV no esta disponible o esta cambiando: {source}")
    if digest == state.get("submitted_sha256"):
        return skip("El contenido del CSV no cambio desde la ultima ejecucion automatica")
    if digest != state.get("candidate_sha256"):
        state["candidate_sha256"] = digest
        return skip("Cambio detectado; esperamos otra lectura igual antes de ejecutar")

    # Una nueva revision permite volver de A -> B -> A: Gold representa el CSV actual.
    # El cursor y run_key evitan duplicados si el daemon se reinicia durante un tick.
    revision = state.get("revision", 0) + 1
    state.update(submitted_sha256=digest, revision=revision)
    return SourceChange(cursor=json.dumps(state, sort_keys=True), sha256=digest,
                        run_key=f"ventas:{revision}:{digest}")
