"""Fusión del crudo para las cargas incrementales.

Es la pieza que hace que releer la última ventana no duplique nada. Va aparte y
sin tocar la red para poder probarla con DataFrames pequeños.
"""
from __future__ import annotations

import pandas as pd


def fusionar(
    existente: pd.DataFrame | None,
    nuevos: pd.DataFrame,
    columnas_clave: list[str],
) -> pd.DataFrame:
    """Une el crudo que ya había con lo recién descargado, sin duplicar filas.

    `columnas_clave` identifican una fila en el origen (el identificador de fila
    que expone Socrata). Cuando una clave se repite gana la versión nueva, que es
    lo correcto si el publicador corrigió un valor: la fila recién descargada es
    la vigente.

    Ojo con lo que NO hace: no deduplica por clave de negocio. El origen publica
    a veces dos filas con el mismo (año, mes, municipio) y valores distintos, y
    esas dos filas son legítimas; un deduplicado por clave de negocio las
    borraría.
    """
    if nuevos is None or nuevos.empty:
        return (existente if existente is not None else pd.DataFrame()).reset_index(drop=True)

    if existente is None or existente.empty:
        return nuevos.reset_index(drop=True)

    faltantes = [columna for columna in columnas_clave if columna not in nuevos.columns]
    if faltantes:
        raise KeyError(f'no puedo fusionar: faltan las columnas {faltantes} en lo descargado')

    combinado = pd.concat([existente, nuevos], ignore_index=True)
    return combinado.drop_duplicates(subset=columnas_clave, keep='last').reset_index(drop=True)
