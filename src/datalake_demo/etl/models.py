"""Contratos de las etapas. Las referencias son opacas para el caso de uso."""

from dataclasses import dataclass
from typing import Literal, TypedDict

from datalake_demo.domain.models import RawSale


@dataclass(frozen=True)
class SourceSnapshot:
    payload: bytes
    rows: list[RawSale]
    sha256: str


@dataclass(frozen=True)
class Execution:
    dataset: str
    execution_id: str
    revision: int
    source_type: str
    created_utc: str


@dataclass(frozen=True)
class Artifact:
    key: str
    sha256: str


@dataclass(frozen=True)
class BronzeResult:
    execution: Execution
    artifact: Artifact
    rows: int


class Quality(TypedDict):
    archivo_bronze: str
    filas_bronze: int
    filas_silver: int
    filas_rechazadas: int
    generado_utc: str


@dataclass(frozen=True)
class SilverResult:
    bronze: BronzeResult
    sales: Artifact
    rejected: Artifact
    quality: Artifact
    rows: int
    rejected_rows: int


@dataclass(frozen=True)
class GoldResult:
    silver: SilverResult
    artifact: Artifact
    groups: int
    revenue_bs: str

    @property
    def execution(self) -> Execution:
        return self.silver.bronze.execution


@dataclass(frozen=True)
class PreparedPublication:
    gold: GoldResult
    manifest: Artifact


@dataclass(frozen=True)
class CurrentPublication:
    execution_id: str
    revision: int
    manifest: Artifact


@dataclass(frozen=True)
class PublicationResult:
    execution_id: str
    revision: int
    status: Literal["published", "already_published", "superseded"]
    manifest: Artifact
    rows: int


def bronze_from_dict(value: dict) -> BronzeResult:
    return BronzeResult(Execution(**value["execution"]), Artifact(**value["artifact"]), value["rows"])


def gold_from_dict(value: dict) -> GoldResult:
    silver = value["silver"]
    result = SilverResult(bronze_from_dict(silver["bronze"]), Artifact(**silver["sales"]),
                          Artifact(**silver["rejected"]), Artifact(**silver["quality"]),
                          silver["rows"], silver["rejected_rows"])
    return GoldResult(result, Artifact(**value["artifact"]), value["groups"], value["revenue_bs"])
