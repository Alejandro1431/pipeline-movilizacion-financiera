"""Perfilado rápido del warehouse: qué hay dentro y qué se puede responder.

No sustituye a un análisis, pero sirve para dos cosas: comprobar de un vistazo
que la carga quedó bien, y tener a mano las cifras para documentar.
"""
from __future__ import annotations

import duckdb

from . import config

CONSULTAS: list[tuple[str, str]] = [
    (
        'Cobertura temporal',
        """
        select 'movilizacion_municipio' as tabla,
               min(periodo)::varchar || ' a ' || max(periodo)::varchar as rango,
               count(distinct (anio, mes))::varchar as periodos,
               count(*)::varchar as filas
        from staging.movilizacion_municipio
        union all
        select 'movilizacion_intermediario',
               min(periodo)::varchar || ' a ' || max(periodo)::varchar,
               count(distinct (anio, mes))::varchar,
               count(*)::varchar
        from staging.movilizacion_intermediario
        union all
        select 'desembolsos_redescuento',
               min(anio)::varchar || ' a ' || max(anio)::varchar,
               count(distinct anio)::varchar,
               count(*)::varchar
        from staging.desembolsos_redescuento
        """,
    ),
    (
        'Top 5 departamentos por valor movilizado (ultimo año completo)',
        """
        with ultimo as (
            select max(anio) as anio
            from marts.mart_movilizacion_departamento_mes
            where mes = 12
        )
        select m.departamento,
               sum(m.desembolsos)::varchar as desembolsos,
               round(sum(m.valor_movilizado) / 1e9, 1)::varchar || ' mil millones' as valor
        from marts.mart_movilizacion_departamento_mes m
        join ultimo u on m.anio = u.anio
        group by 1
        order by sum(m.valor_movilizado) desc
        limit 5
        """,
    ),
    (
        'Tipos de intermediario',
        """
        select tipo_intermediario,
               count(distinct nombre_intermediario)::varchar as entidades,
               sum(desembolsos)::varchar as desembolsos
        from staging.movilizacion_intermediario
        group by 1
        order by sum(desembolsos) desc
        """,
    ),
    (
        'Desembolsos por sector (fuente sucia ya limpiada)',
        """
        select sector,
               count(*)::varchar as registros,
               round(sum(valor_desembolsado) / 1e9, 1)::varchar || ' mil millones' as valor
        from staging.desembolsos_redescuento
        group by 1
        order by sum(valor_desembolsado) desc
        limit 5
        """,
    ),
]


def ejecutar() -> list[tuple[str, list[str], list[tuple]]]:
    resultados = []
    with duckdb.connect(str(config.WAREHOUSE), read_only=True) as conexion:
        for titulo, consulta in CONSULTAS:
            cursor = conexion.execute(consulta)
            columnas = [descripcion[0] for descripcion in cursor.description]
            resultados.append((titulo, columnas, cursor.fetchall()))
    return resultados
