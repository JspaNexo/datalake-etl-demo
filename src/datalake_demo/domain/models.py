"""Datos del negocio: no conocen archivos, bases de datos ni orquestadores."""

from datetime import date
from decimal import Decimal
from typing import TypedDict


class RawSale(TypedDict):
    venta_id: str
    fecha: str
    ciudad: str
    producto: str
    cantidad: str
    precio_unitario: str


class Sale(TypedDict):
    venta_id: int
    fecha: date
    ciudad: str
    producto: str
    cantidad: int
    precio_unitario: Decimal
    importe: Decimal


class GoldRow(TypedDict):
    fecha: date
    ciudad: str
    producto: str
    ventas: int
    unidades: int
    ingresos: Decimal


class RejectedSale(TypedDict):
    registro: int
    datos: dict
    motivo: str
