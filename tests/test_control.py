"""Pruebas de la tabla de control de las cargas incrementales.

Trabajan contra un warehouse temporal, nunca contra el del proyecto. Lo que se
fija aquí es que guardar una marca dos veces actualiza la fila en lugar de
duplicarla, que es de lo que depende que el incremental no se descontrole.
"""
import datetime as dt

import pytest

from pipeline import config, control


@pytest.fixture
def warehouse_temporal(tmp_path, monkeypatch):
    """Redirige el warehouse y las carpetas de datos a un directorio temporal."""
    monkeypatch.setattr(config, 'DATA', tmp_path)
    monkeypatch.setattr(config, 'RAW', tmp_path / 'raw')
    monkeypatch.setattr(config, 'STAGING', tmp_path / 'staging')
    monkeypatch.setattr(config, 'WAREHOUSE', tmp_path / 'prueba.duckdb')
    return tmp_path / 'prueba.duckdb'


class TestMarcas:
    def test_una_fuente_sin_marca_devuelve_none(self, warehouse_temporal):
        assert control.leer_marca('movilizacion_municipio') is None

    def test_guardar_y_volver_a_leer(self, warehouse_temporal):
        cuando = dt.datetime(2026, 10, 1)

        control.guardar_marca('movilizacion_municipio', 'fecha_de_informaci_n', cuando, 9_770, 179_500)

        assert control.leer_marca('movilizacion_municipio') == cuando

    def test_guardar_dos_veces_actualiza_y_no_duplica(self, warehouse_temporal):
        control.guardar_marca('m', 'fecha', dt.datetime(2026, 1, 1), 10, 100)
        control.guardar_marca('m', 'fecha', dt.datetime(2026, 2, 1), 20, 120)

        filas = control.estado()

        assert len(filas) == 1
        assert filas[0]['ultimo_valor'] == dt.datetime(2026, 2, 1)
        assert filas[0]['filas_ultima_carga'] == 20
        assert filas[0]['filas_totales'] == 120

    def test_una_fuente_sin_columna_de_fecha_se_registra_igual(self, warehouse_temporal):
        control.guardar_marca('desembolsos_redescuento', None, None, 2_982, 2_982)

        fila = control.estado()[0]

        assert fila['columna_fecha'] is None
        assert fila['ultimo_valor'] is None


class TestEstado:
    def test_empieza_vacio(self, warehouse_temporal):
        assert control.estado() == []

    def test_devuelve_las_fuentes_ordenadas(self, warehouse_temporal):
        control.guardar_marca('z', None, None, 1, 1)
        control.guardar_marca('a', None, None, 1, 1)

        assert [fila['fuente'] for fila in control.estado()] == ['a', 'z']
