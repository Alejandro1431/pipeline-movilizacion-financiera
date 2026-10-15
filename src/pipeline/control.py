"""Tabla de control de las cargas incrementales (marcas de agua).

La marca de agua es el último valor de la columna de fecha que se trajo del
origen: en la siguiente carga se pide todo lo publicado a partir de ahí.

Se relee la última ventana a propósito (`>=`, no `>`), porque los publicadores
corrigen datos hacia atrás y una marca exclusiva se perdería esas correcciones.
Releer no duplica filas: la fusión va por el identificador de fila del origen.
"""
from __future__ import annotations

import datetime as dt
import logging

import duckdb

from . import config

log = logging.getLogger(__name__)

ESQUEMA = 'control'
TABLA = 'cargas'


def conectar() -> duckdb.DuckDBPyConnection:
    """Abre el warehouse y se asegura de que exista la tabla de control."""
    config.preparar_directorios()
    conexion = duckdb.connect(str(config.WAREHOUSE))
    conexion.execute(f'create schema if not exists {ESQUEMA}')
    conexion.execute(f"""
        create table if not exists {ESQUEMA}.{TABLA} (
            fuente              varchar primary key,
            columna_fecha       varchar,
            ultimo_valor        timestamp,
            filas_ultima_carga  bigint,
            filas_totales       bigint,
            actualizado_en      timestamp
        )
    """)
    return conexion


def leer_marca(fuente: str) -> dt.datetime | None:
    """Último valor de la columna de fecha registrado para una fuente."""
    with conectar() as conexion:
        fila = conexion.execute(
            f'select ultimo_valor from {ESQUEMA}.{TABLA} where fuente = ?', [fuente]
        ).fetchone()
    return fila[0] if fila else None


def guardar_marca(
    fuente: str,
    columna_fecha: str | None,
    ultimo_valor: dt.datetime | None,
    filas_carga: int,
    filas_totales: int,
) -> None:
    """Registra (o actualiza) la marca de agua de una fuente."""
    # Se borra e inserta en lugar de usar ON CONFLICT: es igual de correcto aquí
    # y no depende de la versión de DuckDB.
    with conectar() as conexion:
        conexion.execute(f'delete from {ESQUEMA}.{TABLA} where fuente = ?', [fuente])
        conexion.execute(
            f'insert into {ESQUEMA}.{TABLA} values (?, ?, ?, ?, ?, ?)',
            [fuente, columna_fecha, ultimo_valor, filas_carga, filas_totales, dt.datetime.now()],
        )


def estado() -> list[dict]:
    """Contenido de la tabla de control, para inspeccionarla desde la consola."""
    with conectar() as conexion:
        cursor = conexion.execute(f'select * from {ESQUEMA}.{TABLA} order by fuente')
        columnas = [descripcion[0] for descripcion in cursor.description]
        filas = cursor.fetchall()
    return [dict(zip(columnas, fila, strict=True)) for fila in filas]
