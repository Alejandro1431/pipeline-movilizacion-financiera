"""Rutas y configuración del proyecto.

Todo se resuelve a partir de la raíz del repositorio, así que el pipeline
funciona igual sin importar desde dónde se ejecute.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
DATA = RAIZ / 'data'
RAW = DATA / 'raw'
STAGING = DATA / 'staging'
WAREHOUSE = DATA / 'warehouse.duckdb'
SQL_DIR = RAIZ / 'sql'
ARCHIVO_FUENTES = RAIZ / 'config' / 'sources.json'


@lru_cache(maxsize=1)
def fuentes() -> dict:
    """Definición de las fuentes de datos (config/sources.json)."""
    with open(ARCHIVO_FUENTES, encoding='utf-8') as archivo:
        return json.load(archivo)


def fuente(nombre: str) -> dict:
    definidas = fuentes()
    if nombre not in definidas:
        disponibles = ', '.join(sorted(definidas))
        raise KeyError(f'Fuente desconocida: {nombre}. Disponibles: {disponibles}')
    return definidas[nombre]


def preparar_directorios() -> None:
    for ruta in (DATA, RAW, STAGING):
        ruta.mkdir(parents=True, exist_ok=True)


def ruta_sql(nombre: str) -> Path:
    return SQL_DIR / f'{nombre}.sql'
