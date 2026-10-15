"""Reglas de calidad sobre las tablas de staging.

Cada regla es una consulta que devuelve un único número: cuántos registros
violan la regla. Lo esperado casi siempre es 0. Si alguna falla, el proceso
termina con código 1, que es lo que permite usarlo como puerta en CI.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import duckdb

from . import almacen, config

log = logging.getLogger(__name__)


@dataclass
class Regla:
    tabla: str
    nombre: str
    consulta: str
    esperado: int = 0
    minimo: int | None = None
    maximo: int | None = None

    def evaluar(self, conexion: duckdb.DuckDBPyConnection) -> tuple[bool, int]:
        valor = int(conexion.execute(self.consulta).fetchone()[0] or 0)
        if self.minimo is not None:
            return valor >= self.minimo, valor
        if self.maximo is not None:
            return valor <= self.maximo, valor
        return valor == self.esperado, valor


def reglas(anio_maximo: int) -> list[Regla]:
    return [
        # --- Movilización por municipio ---
        Regla(
            'movilizacion_municipio',
            'la clave (año, mes, municipio) no tiene nulos',
            'select count(*) from staging.movilizacion_municipio '
            'where anio is null or mes is null or codigo_municipio is null',
        ),
        Regla(
            'movilizacion_municipio',
            'la clave (año, mes, municipio) es única',
            'select count(*) - count(distinct (anio, mes, codigo_municipio)) '
            'from staging.movilizacion_municipio',
        ),
        Regla(
            'movilizacion_municipio',
            'no hay valores negativos',
            'select count(*) from staging.movilizacion_municipio '
            'where desembolsos < 0 or clientes < 0 or valor_movilizado < 0',
        ),
        Regla(
            'movilizacion_municipio',
            'los años están en el rango esperado',
            f'select count(*) from staging.movilizacion_municipio '
            f'where anio < 2009 or anio > {anio_maximo}',
        ),
        Regla(
            'movilizacion_municipio',
            'hay periodos suficientes para analizar',
            'select count(distinct (anio, mes)) from staging.movilizacion_municipio',
            minimo=150,
        ),
        Regla(
            'movilizacion_municipio',
            'las filas que hubo que agregar siguen siendo marginales',
            'select count(*) from staging.movilizacion_municipio where filas_origen > 1',
            maximo=200,
        ),
        # --- Movilización por intermediario ---
        Regla(
            'movilizacion_intermediario',
            'la clave (año, mes, intermediario) es única',
            'select count(*) - count(distinct (anio, mes, nombre_intermediario)) '
            'from staging.movilizacion_intermediario',
        ),
        Regla(
            'movilizacion_intermediario',
            'no hay valores negativos',
            'select count(*) from staging.movilizacion_intermediario '
            'where desembolsos < 0 or clientes < 0 or total_desembolso < 0',
        ),
        # --- Desembolsos (fuente sucia ya limpiada) ---
        Regla(
            'desembolsos_redescuento',
            'ningún valor quedó sin parsear',
            'select count(*) from staging.desembolsos_redescuento where valor_desembolsado is null',
        ),
        Regla(
            'desembolsos_redescuento',
            'los valores son positivos',
            'select count(*) from staging.desembolsos_redescuento where valor_desembolsado <= 0',
        ),
        Regla(
            'desembolsos_redescuento',
            'las coordenadas caen dentro de Colombia',
            'select count(*) from staging.desembolsos_redescuento '
            'where longitud is not null and (longitud < -82 or longitud > -66 '
            'or latitud < -5 or latitud > 14)',
        ),
        Regla(
            'desembolsos_redescuento',
            'la mayoría de filas trae coordenadas',
            'select count(*) from staging.desembolsos_redescuento where longitud is not null',
            minimo=1,
        ),
        # --- Crudo: la fusión de las cargas incrementales no puede dejar repetidos ---
        Regla(
            'raw',
            'el crudo de municipios no tiene identificadores de fila repetidos',
            'select count(*) - count(distinct id_fila) from raw.movilizacion_municipio',
        ),
        Regla(
            'raw',
            'el crudo de intermediarios no tiene identificadores de fila repetidos',
            'select count(*) - count(distinct id_fila) from raw.movilizacion_intermediario',
        ),
        # --- Control de cargas incrementales ---
        Regla(
            'control',
            'ninguna marca de agua está en el futuro',
            'select count(*) from control.cargas where ultimo_valor > now()',
        ),
    ]


def ejecutar(anio_maximo: int | None = None, guardar: bool = True) -> dict:
    """Corre todas las reglas y devuelve el informe."""
    if anio_maximo is None:
        from datetime import date
        anio_maximo = date.today().year

    resultados = []
    with duckdb.connect(str(config.WAREHOUSE)) as conexion:
        # Las reglas del crudo leen las vistas raw.*, que apuntan al almacén
        # activo (disco o bucket), así que la conexión necesita su configuración.
        almacen.actual().configurar(conexion)
        for regla in reglas(anio_maximo):
            try:
                pasa, valor = regla.evaluar(conexion)
            except Exception as error:  # noqa: BLE001
                pasa, valor = False, -1
                log.error('la regla "%s" no se pudo evaluar: %s', regla.nombre, error)
            resultados.append({'tabla': regla.tabla, 'regla': regla.nombre, 'valor': valor, 'pasa': pasa})

    fallidas = [r for r in resultados if not r['pasa']]
    informe = {
        'total': len(resultados),
        'fallidas': len(fallidas),
        'resultados': resultados,
        'veredicto': 'pass' if not fallidas else 'fail',
    }

    if guardar:
        destino = config.DATA / 'calidad.json'
        destino.parent.mkdir(parents=True, exist_ok=True)
        with open(destino, 'w', encoding='utf-8') as archivo:
            json.dump(informe, archivo, ensure_ascii=False, indent=2)

    return informe
