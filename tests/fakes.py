"""Adaptadores de prueba; la publicación modela una confirmación atómica."""

import copy
import hashlib
import json
from dataclasses import asdict
from decimal import Decimal

from datalake_demo.etl.models import (
    Artifact, BronzeResult, CurrentPublication, Execution, GoldResult,
    PreparedPublication, PublicationResult, SilverResult,
)


class MemoryLake:
    def __init__(self):
        self.objects = {}
        self.fail_at = None

    def _write(self, key, value, stage):
        if self.fail_at == stage:
            raise OSError("Fallo simulado: " + stage)
        digest = hashlib.sha256(json.dumps(value, default=str, sort_keys=True).encode()).hexdigest()
        artifact = Artifact(key, digest)
        if key in self.objects and self.objects[key] != value:
            raise ValueError("Intento de modificar un objeto inmutable")
        self.objects[key] = copy.deepcopy(value)
        return artifact

    def _read(self, artifact):
        return copy.deepcopy(self.objects[artifact.key])

    def write_bronze(self, execution, snapshot):
        artifact = self._write("bronze/" + execution.dataset + "/" + snapshot.sha256,
                               snapshot.rows, "bronze")
        return BronzeResult(execution, artifact, len(snapshot.rows))

    def read_bronze(self, bronze):
        return self._read(bronze.artifact)

    def write_silver(self, bronze, sales, rejected, quality):
        prefix = bronze.execution.dataset + "/" + bronze.execution.execution_id + "/"
        a = self._write(prefix + "silver", sales, "silver")
        b = self._write(prefix + "rejected", rejected, "rejected")
        c = self._write(prefix + "quality", quality, "quality")
        return SilverResult(bronze, a, b, c, len(sales), len(rejected))

    def read_silver(self, silver):
        return self._read(silver.sales)

    def write_gold(self, silver, rows):
        execution = silver.bronze.execution
        artifact = self._write(execution.dataset + "/" + execution.execution_id + "/gold", rows, "gold")
        return GoldResult(silver, artifact, len(rows), str(sum((r["ingresos"] for r in rows), Decimal("0.00"))))

    def prepare(self, gold):
        artifact = self._write(gold.artifact.key + "/manifest", asdict(gold), "manifest")
        return PreparedPublication(gold, artifact)

    def read_publication(self, prepared):
        assert self._read(prepared.manifest) == asdict(prepared.gold)
        return self._read(prepared.gold.artifact), self._read(prepared.gold.silver.quality)


class MemoryPublications:
    def __init__(self):
        self.executions = {}
        self.snapshots = {}
        self.publications = {}
        self.rows = {}
        self.quality = {}
        self.fail = False
        self.commits = 0

    def begin(self, dataset, execution_id, source_type, created_utc):
        key = (dataset, execution_id)
        if key not in self.executions:
            self.executions[key] = Execution(dataset, execution_id, len(self.executions) + 1, source_type, created_utc)
        return self.executions[key]

    def captured(self, execution):
        return self.snapshots.get((execution.dataset, execution.execution_id))

    def bind_bronze(self, bronze):
        key = (bronze.execution.dataset, bronze.execution.execution_id)
        return self.snapshots.setdefault(key, bronze)

    def current(self, dataset):
        return self.publications.get(dataset)

    def publish(self, prepared, rows, quality):
        execution = prepared.gold.execution
        current = self.current(execution.dataset)
        status = "published"
        if current and current.revision > execution.revision:
            status = "superseded"
        elif current and current.revision == execution.revision:
            assert current.manifest == prepared.manifest
            status = "already_published"
        else:
            if self.fail:
                raise OSError("Fallo simulado antes del commit")
            self.rows[execution.dataset] = copy.deepcopy(rows)
            self.quality[execution.dataset] = copy.deepcopy(quality)
            self.publications[execution.dataset] = CurrentPublication(execution.execution_id, execution.revision, prepared.manifest)
            self.commits += 1
        return PublicationResult(execution.execution_id, execution.revision, status, prepared.manifest, len(rows))
