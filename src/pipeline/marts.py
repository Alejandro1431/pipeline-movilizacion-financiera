"""Modelo dimensional con dbt.

dbt se encarga de la capa de marts (dimensiones, hechos y agregados) sobre las
tablas de staging que construye el pipeline de Python. Aquí solo se lanza el CLI
con la configuración del repositorio, para que no dependa de la configuración
global de dbt de cada máquina.
"""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from . import config

log = logging.getLogger(__name__)


def ejecutable() -> str:
    """Localiza el CLI de dbt del entorno activo."""
    candidato = Path(sys.executable).with_name('dbt.exe' if os.name == 'nt' else 'dbt')
    if candidato.exists():
        return str(candidato)

    encontrado = shutil.which('dbt')
    if encontrado:
        return encontrado

    raise RuntimeError(
        'No encuentro el CLI de dbt. Instala las dependencias: pip install -r requirements.txt'
    )


def proyecto() -> Path:
    return config.RAIZ / 'dbt'


def ejecutar(comandos: tuple[str, ...] = ('run', 'test')) -> dict[str, int]:
    """Ejecuta los comandos de dbt en orden. Se detiene en el primer fallo."""
    dbt = ejecutable()
    ruta_proyecto = proyecto()

    entorno = os.environ.copy()
    entorno['PIPELINE_DUCKDB'] = str(config.WAREHOUSE)

    resultados: dict[str, int] = {}
    for comando in comandos:
        log.info('dbt %s', comando)
        proceso = subprocess.run(
            [dbt, comando, '--project-dir', str(ruta_proyecto), '--profiles-dir', str(ruta_proyecto)],
            cwd=str(config.RAIZ),
            env=entorno,
            check=False,
        )
        resultados[comando] = proceso.returncode
        if proceso.returncode != 0:
            log.error('dbt %s termino con codigo %s', comando, proceso.returncode)
            break

    return resultados
