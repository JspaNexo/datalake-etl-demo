"""Reglas del negocio, sin depender de MinIO, PostgreSQL ni Dagster."""

import re
import unicodedata
from datetime import date
from decimal import Decimal, InvalidOperation

from datalake_demo.domain.models import GoldRow, RawSale, RejectedSale, Sale

SALE_FIELDS = tuple(RawSale.__annotations__)

CENT = Decimal("0.01")
MAX_MONEY = Decimal("9999999999999999.99")


def normalize_text(value: str) -> str:
    text = " ".join(value.split())
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def positive_integer(value: str, field: str) -> int:
    if not re.fullmatch(r"[0-9]+", value):
        raise ValueError(f"{field}: debe ser un entero positivo")
    number = int(value)
    if number <= 0 or number > 2_147_483_647:
        raise ValueError(f"{field}: fuera del rango de enteros positivos")
    return number


def clean_sales(rows: list[RawSale]) -> tuple[list[Sale], list[RejectedSale]]:
    valid, rejected, seen = [], [], set()
    for line, raw in enumerate(rows, start=1):
        try:
            if None in raw or any(raw.get(column) is None for column in SALE_FIELDS):
                raise ValueError("estructura de registro invalida")
            values = {column: raw[column].strip() for column in SALE_FIELDS}
            if any(not value for value in values.values()):
                raise ValueError("campo obligatorio vacio")
            sale_id = positive_integer(values["venta_id"], "venta_id")
            quantity = positive_integer(values["cantidad"], "cantidad")
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", values["fecha"]):
                raise ValueError("fecha: usar YYYY-MM-DD")
            sale_date = date.fromisoformat(values["fecha"])
            price = Decimal(values["precio_unitario"])
            if not price.is_finite() or price <= 0 or price > MAX_MONEY:
                raise ValueError("precio_unitario: debe ser un importe positivo finito")
            if price != price.quantize(CENT):
                raise ValueError("precio_unitario: maximo dos decimales")
            amount = price * quantity
            if amount > MAX_MONEY:
                raise ValueError("importe: fuera del rango permitido")
            city = normalize_text(values["ciudad"]).title()
            product = normalize_text(values["producto"]).lower()
            if not city or not product:
                raise ValueError("ciudad y producto: texto vacio tras normalizar")
            if sale_id in seen:
                raise ValueError("venta_id duplicado; se conserva la primera fila valida")
            seen.add(sale_id)
            valid.append({
                "venta_id": sale_id,
                "fecha": sale_date,
                "ciudad": city,
                "producto": product,
                "cantidad": quantity,
                "precio_unitario": price.quantize(CENT),
                "importe": amount.quantize(CENT),
            })
        except (ValueError, InvalidOperation) as error:
            rejected.append({"registro": line, "datos": raw, "motivo": str(error)})
    return valid, rejected


def aggregate_sales(rows: list[Sale]) -> list[GoldRow]:
    groups = {}
    for row in rows:
        key = (row["fecha"], row["ciudad"], row["producto"])
        group = groups.setdefault(key, {
            "fecha": key[0], "ciudad": key[1], "producto": key[2],
            "ventas": 0, "unidades": 0, "ingresos": Decimal("0.00"),
        })
        group["ventas"] += 1
        group["unidades"] += row["cantidad"]
        group["ingresos"] += row["importe"]
        if group["ingresos"] > MAX_MONEY:
            raise ValueError("ingresos agregados: fuera del rango permitido")
    return [groups[key] for key in sorted(groups)]

