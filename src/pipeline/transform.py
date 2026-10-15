"""Transformación: Parquet crudo -> tablas de staging tipadas.

Decisión de diseño: las dos fuentes limpias se resuelven con SQL (es lo que
mejor expresa un tipado y una normalización), y la fuente sucia se resuelve en
Python porque ahí sí hace falta lógica probada con tests. Se podría hacer todo
con funciones de Python dentro de DuckDB, pero se perdería poder probarlo sin
levantar la base.
"""
from __future__ import annotations

import logging

import duckdb
import pandas as pd

from . import almacen, config
from .limpieza import extraer_coordenadas, limpiar_texto, parsear_moneda

log = logging.getLogger(__name__)

FUENTE_SUCIA = 'desembolsos_redescuento'


def conectar() -> duckdb.DuckDBPyConnection:
    """Abre (o crea) el warehouse DuckDB y sus esquemas."""
    config.preparar_directorios()
    conexion = duckdb.connect(str(config.WAREHOUSE))
    conexion.execute('create schema if not exists raw')
    conexion.execute('create schema if not exists staging')
    return conexion


def vistas_raw(conexion: duckdb.DuckDBPyConnection) -> None:
    """Expone cada fuente cruda como vista, leyendo el Parquet del almacén activo.

    La vista apunta al almacén configurado (disco o bucket) y se reconstruye en
    cada ejecución, así que el modo tiene que ser el mismo al transformar y al
    consultar el crudo. El comando `all` lo garantiza: usa un solo almacén de
    principio a fin.
    """
    tienda = almacen.actual()
    tienda.configurar(conexion)

    for nombre in config.fuentes():
        conexion.execute(
            f'create or replace view raw.{nombre} as '
            f"select * from read_parquet('{tienda.uri(nombre)}', "
            f'hive_partitioning = true, union_by_name = true)'
        )


def aplicar_sql(conexion: duckdb.DuckDBPyConnection) -> list[str]:
    """Ejecuta cada sql/staging_*.sql y devuelve las tablas construidas."""
    construidas: list[str] = []
    for archivo in sorted(config.SQL_DIR.glob('staging_*.sql')):
        # El archivo se llama staging_<tabla>.sql y crea staging.<tabla>.
        tabla = archivo.stem.removeprefix('staging_')
        log.info('aplicando %s', archivo.name)
        conexion.execute(archivo.read_text(encoding='utf-8'))
        construidas.append(tabla)
    return construidas


def limpiar_desembolsos(conexion: duckdb.DuckDBPyConnection) -> str:
    """Normaliza la fuente sucia y la deja como tabla de staging.

    Dos problemas reales del origen:
      1. `valor_desembolsado` llega como texto con formato de moneda.
      2. `ubicacion_longitud_latitud` llega como objeto GeoJSON anidado.
    """
    crudo = conexion.execute(f'select * from raw.{FUENTE_SUCIA}').df()

    limpio = pd.DataFrame({
        'anio': pd.to_numeric(crudo.get('ano'), errors='coerce').astype('Int64'),
        'departamento': crudo.get('departamento', pd.Series(dtype='object')).map(limpiar_texto),
        'sector': crudo.get('sector', pd.Series(dtype='object')).map(limpiar_texto),
        'valor_desembolsado': crudo.get('valor_desembolsado', pd.Series(dtype='object')).map(parsear_moneda),
    })

    geometria = crudo.get('ubicacion_longitud_latitud', pd.Series(dtype='object'))
    coordenadas = geometria.map(extraer_coordenadas)
    limpio['longitud'] = [par[0] for par in coordenadas]
    limpio['latitud'] = [par[1] for par in coordenadas]

    limpio = limpio.dropna(subset=['anio', 'valor_desembolsado'])
    limpio['anio'] = limpio['anio'].astype('int64')

    destino = config.STAGING / f'{FUENTE_SUCIA}.parquet'
    destino.parent.mkdir(parents=True, exist_ok=True)
    limpio.to_parquet(destino, index=False)

    conexion.execute(f'create or replace table staging.{FUENTE_SUCIA} as select * from read_parquet(\'{destino.as_posix()}\')')
    log.info('%s: %s filas limpias', FUENTE_SUCIA, f'{len(limpio):,}'.replace(',', '.'))

    descartadas = len(crudo) - len(limpio)
    if descartadas:
        log.warning('%s: %s filas descartadas por valor invalido o año nulo', FUENTE_SUCIA, descartadas)
    return FUENTE_SUCIA


def ejecutar() -> dict:
    """Construye todas las tablas de staging."""
    with conectar() as conexion:
        vistas_raw(conexion)
        tablas = aplicar_sql(conexion)
        tablas.append(limpiar_desembolsos(conexion))

        conteos = {}
        for tabla in tablas:
            filas = conexion.execute(f'select count(*) from staging.{tabla}').fetchone()[0]
            conteos[tabla] = filas

    return {'tablas': conteos, 'warehouse': str(config.WAREHOUSE)}
