from dataclasses import dataclass

CSV_COLUMNS = ("venta_id", "fecha", "ciudad", "producto", "cantidad", "precio_unitario")


@dataclass(frozen=True)
class ExtractedSales:
    """Contenido original, filas leidas e identidad del archivo de entrada."""

    payload: bytes
    rows: list[dict]
    sha256: str

    @property
    def bronze_key(self) -> str:
        return f"bronze/ventas/{self.sha256}.csv"


@dataclass(frozen=True)
class SilverBatch:
    """Resultado de limpiar un archivo: ventas aceptadas, rechazos y calidad."""

    sales: list[dict]
    rejected: list[dict]
    quality: dict

