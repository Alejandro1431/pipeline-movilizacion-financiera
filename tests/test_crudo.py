"""Pruebas de la fusión del crudo en las cargas incrementales.

Es la lógica que decide si releer la última ventana duplica filas o no, así que
se prueba con DataFrames pequeños: sin red, sin DuckDB y sin disco.
"""
import pandas as pd
import pytest

from pipeline.crudo import fusionar


def marco(filas: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(filas)


class TestFusionar:
    def test_sin_crudo_previo_devuelve_lo_descargado(self):
        nuevos = marco([{'id_fila': 'a', 'valor': 1}])
        assert fusionar(None, nuevos, ['id_fila']).to_dict('records') == [{'id_fila': 'a', 'valor': 1}]

    def test_lo_nuevo_gana_cuando_la_clave_se_repite(self):
        existente = marco([{'id_fila': 'a', 'valor': 1}, {'id_fila': 'b', 'valor': 2}])
        nuevos = marco([{'id_fila': 'a', 'valor': 99}])

        resultado = fusionar(existente, nuevos, ['id_fila'])

        assert len(resultado) == 2
        assert resultado.set_index('id_fila').loc['a', 'valor'] == 99

    def test_conserva_las_filas_que_no_vinieron_en_la_ventana(self):
        existente = marco([{'id_fila': 'a', 'valor': 1}, {'id_fila': 'b', 'valor': 2}])
        nuevos = marco([{'id_fila': 'c', 'valor': 3}])

        assert set(fusionar(existente, nuevos, ['id_fila'])['id_fila']) == {'a', 'b', 'c'}

    def test_no_deduplica_por_clave_de_negocio(self):
        # El origen publica a veces dos filas con el mismo (año, mes, municipio) y
        # valores distintos. Las dos son legítimas: deduplicar por clave de negocio
        # las borraría, y eso es justo lo que no debe pasar.
        existente = marco([
            {'id_fila': 'r1', 'a_o': 2026, 'mes': 9, 'cod_mun': '05631', 'total': 120},
            {'id_fila': 'r2', 'a_o': 2026, 'mes': 9, 'cod_mun': '05631', 'total': 1},
        ])
        nuevos = marco([{'id_fila': 'r3', 'a_o': 2026, 'mes': 9, 'cod_mun': '05631', 'total': 7}])

        assert len(fusionar(existente, nuevos, ['id_fila'])) == 3

    def test_sin_novedades_devuelve_lo_que_ya_habia(self):
        existente = marco([{'id_fila': 'a', 'valor': 1}])
        assert len(fusionar(existente, pd.DataFrame(), ['id_fila'])) == 1

    def test_falla_si_lo_descargado_no_trae_la_columna_clave(self):
        existente = marco([{'id_fila': 'a', 'valor': 1}])
        with pytest.raises(KeyError, match='id_fila'):
            fusionar(existente, marco([{'otra': 1}]), ['id_fila'])
