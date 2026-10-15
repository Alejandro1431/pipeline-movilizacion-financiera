"""Orquestación del pipeline con Prefect.

Aquí el pipeline deja de ser una secuencia de funciones y pasa a ser un flujo con
dependencias explícitas entre tareas, reintentos donde tiene sentido y un
historial de ejecuciones consultable.

Por qué Prefect y no Airflow o Dagster
--------------------------------------
- **Airflow** es el orquestador que más aparece en las ofertas, pero en Windows
  exige contenedores (Docker + WSL2) y un stack de varios servicios. Para un
  proyecto de portafolio que tiene que ser reproducible con un comando, es
  desproporcionado. La migración sería directa: cada tarea de aquí es un operador
  allí y el grafo de dependencias es el mismo.
- **Dagster** encaja muy bien cuando el pipeline se piensa como activos de datos,
  pero su modelo de recursos y su instalación pesan más de lo que el proyecto
  necesita hoy.
- **Prefect** se instala con pip, corre en Windows sin contenedores y expone las
  mismas ideas: tareas, dependencias, reintentos, programación e interfaz de
  seguimiento.

Nota sobre el paralelismo
-------------------------
Las tareas van una detrás de otra y no por comodidad. El modelo dimensional y las
reglas de calidad dependen los dos del staging, así que se podrían lanzar a la vez,
pero DuckDB deja un solo proceso escritor por archivo: dos tareas concurrentes
sobre el mismo warehouse se pisarían.
"""
from __future__ import annotations

import logging
import os

# Prefect funciona en modo efímero (sin servidor) y sin enviar telemetría:
# el flujo tiene que poder correr en el equipo de cualquiera y en CI.
os.environ.setdefault('PREFECT_SERVER_ANALYTICS_ENABLED', 'false')
os.environ.setdefault('PREFECT_LOGGING_LEVEL', 'INFO')

from prefect import flow, task  # noqa: E402

from . import config, extract, marts, perfil, quality, transform  # noqa: E402

log = logging.getLogger(__name__)


@task(name='extraer', retries=3, retry_delay_seconds=15, log_prints=True)
def tarea_extraer(desde: str | None = None, incremental: bool = False) -> dict:
    """Descarga las fuentes. Es la única tarea con red, así que es la que reintenta."""
    config.preparar_directorios()
    resumenes = extract.extraer_todas(desde=desde, incremental=incremental)
    total = sum(resumen['filas'] for resumen in resumenes)
    nuevas = sum(resumen.get('filas_nuevas', 0) for resumen in resumenes)
    for resumen in resumenes:
        print(f"  {resumen['fuente']}: {resumen['filas']} filas | {resumen['modo']}")
    print(f'extraidas {total:,} filas y {nuevas:,} nuevas de {len(resumenes)} fuentes'.replace(',', '.'))
    return {'fuentes': len(resumenes), 'filas': total, 'filas_nuevas': nuevas}


@task(name='transformar', log_prints=True)
def tarea_transformar() -> dict:
    """Construye el staging tipado en DuckDB."""
    resultado = transform.ejecutar()
    for tabla, filas in resultado['tablas'].items():
        print(f'  {tabla}: {filas:,} filas'.replace(',', '.'))
    return resultado['tablas']


@task(name='modelo dimensional', log_prints=True)
def tarea_marts() -> dict:
    """Construye y prueba el modelo dimensional con dbt."""
    resultados = marts.ejecutar()
    fallidos = [comando for comando, codigo in resultados.items() if codigo != 0]
    if fallidos:
        raise RuntimeError(f'dbt fallo en: {", ".join(fallidos)}')
    return resultados


@task(name='calidad', log_prints=True)
def tarea_calidad() -> dict:
    """Corre las reglas de calidad. Si alguna falla, la tarea falla y el flujo para."""
    informe = quality.ejecutar()
    print(f"  {informe['total'] - informe['fallidas']}/{informe['total']} reglas pasan")
    if informe['veredicto'] != 'pass':
        fallidas = [r['regla'] for r in informe['resultados'] if not r['pasa']]
        raise ValueError(f'reglas de calidad incumplidas: {"; ".join(fallidas)}')
    return informe


@task(name='perfilar', log_prints=True)
def tarea_perfilar() -> int:
    """Imprime las cifras de negocio que salen del warehouse."""
    secciones = perfil.ejecutar()
    for titulo, _columnas, filas in secciones:
        print(f'  {titulo}: {len(filas)} filas')
    return len(secciones)


@flow(name='pipeline-movilizacion-financiera', log_prints=True)
def pipeline(desde: str | None = None, incremental: bool = False) -> dict:
    """Flujo completo: extraer, transformar, modelar, validar y perfilar.

    Args:
        desde: fecha ISO para una carga desde una fecha concreta.
        incremental: usa las marcas de agua del warehouse en lugar de recargar todo.
    """
    print('flujo: extraer -> transformar -> modelo dimensional -> calidad -> perfilar')

    extraccion = tarea_extraer(desde, incremental)
    staging = tarea_transformar()
    modelo = tarea_marts()
    informe = tarea_calidad()
    secciones = tarea_perfilar()

    return {
        'filas_extraidas': extraccion['filas'],
        'filas_nuevas': extraccion['filas_nuevas'],
        'tablas_de_staging': len(staging),
        'pasos_de_dbt': len(modelo),
        'reglas_de_calidad': informe['total'],
        'secciones_perfiladas': secciones,
    }


def servir(cron: str = '0 6 * * 1') -> None:
    """Deja el flujo servido con una programación semanal.

    Requiere el servidor local de Prefect (`prefect server start`) o una API
    configurada; en modo efímero solo sirve para una ejecución.
    """
    pipeline.serve(name='semanal', cron=cron, tags=['produccion'])
