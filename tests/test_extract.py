"""Pruebas de la extracción.

Nada de esto sale a la red: se prueban las piezas que deciden qué se pide al
origen y qué se avisa cuando el esquema cambia, con un cliente de Socrata falso y
un crudo temporal en disco.
"""
import datetime as dt

import pandas as pd
import pytest

from pipeline import almacen, config, extract

DEFINICION = {'dataset': 'xcga-ji4i', 'columna_id': 'id_fila', 'particion': 'a_o'}


class ClienteFalso:
    """Cliente mínimo: solo lo que hace falta para construir la consulta."""

    def __init__(self, columnas):
        self._columnas = columnas

    def columnas(self):
        return self._columnas


@pytest.fixture
def crudo_temporal(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'DATA', tmp_path)
    monkeypatch.setattr(config, 'RAW', tmp_path / 'raw')
    monkeypatch.setattr(config, 'STAGING', tmp_path / 'staging')
    monkeypatch.setattr(config, 'WAREHOUSE', tmp_path / 'prueba.duckdb')
    monkeypatch.delenv('PIPELINE_ALMACEN', raising=False)
    return tmp_path / 'raw'


class TestSeleccion:
    def test_pide_el_identificador_renombrado_mas_las_columnas(self):
        seleccion = extract._seleccion(ClienteFalso(['a_o', 'mes']), DEFINICION)
        assert seleccion == ':id as id_fila, a_o, mes'

    def test_un_origen_sin_esquema_es_un_error_del_pipeline(self):
        with pytest.raises(RuntimeError, match='xcga-ji4i'):
            extract._seleccion(ClienteFalso([]), DEFINICION)


class TestUltimaFecha:
    def test_devuelve_la_fecha_mayor(self):
        datos = pd.DataFrame({'fecha_de_informaci_n': ['2026-01-01', '2026-09-01', '2026-05-01']})
        assert extract._ultima_fecha(datos, 'fecha_de_informaci_n') == dt.datetime(2026, 9, 1)

    def test_sin_columna_configurada_devuelve_none(self):
        assert extract._ultima_fecha(pd.DataFrame({'x': [1]}), None) is None

    def test_una_columna_que_no_vino_devuelve_none(self):
        assert extract._ultima_fecha(pd.DataFrame({'x': [1]}), 'fecha') is None

    def test_un_marco_vacio_devuelve_none(self):
        assert extract._ultima_fecha(pd.DataFrame(), 'fecha') is None

    def test_las_fechas_ilegibles_no_cuentan(self):
        datos = pd.DataFrame({'fecha': ['no es una fecha', '2025-03-04']})
        with pytest.warns(UserWarning, match='Could not infer format'):
            assert extract._ultima_fecha(datos, 'fecha') == dt.datetime(2025, 3, 4)


class TestMiles:
    def test_separa_los_miles_con_punto(self):
        assert extract._miles(179_500) == '179.500'


class TestDerivaDeEsquema:
    def _dejar_crudo(self, columnas):
        datos = pd.DataFrame([dict.fromkeys(columnas, 'x')])
        datos['a_o'] = 2024
        almacen.AlmacenLocal().escribir('prueba', datos, 'a_o')

    def test_avisa_de_las_columnas_nuevas(self, crudo_temporal, caplog):
        self._dejar_crudo(['id_fila', 'mes'])
        nuevos = pd.DataFrame([{'id_fila': 'x', 'mes': '1', 'sector': 'Salud', 'a_o': 2024}])

        with caplog.at_level('INFO'):
            extract._avisar_de_cambios_de_esquema('prueba', nuevos, 'a_o', 'id_fila')

        assert 'columnas nuevas' in caplog.text
        assert 'sector' in caplog.text

    def test_avisa_cuando_desaparece_el_identificador_de_fila(self, crudo_temporal, caplog):
        self._dejar_crudo(['id_fila', 'mes'])
        nuevos = pd.DataFrame([{'mes': '1', 'sector': 'Salud', 'a_o': 2024}])

        with caplog.at_level('WARNING'):
            extract._avisar_de_cambios_de_esquema('prueba', nuevos, 'a_o', 'id_fila')

        assert 'identificador de fila' in caplog.text

    def test_sin_crudo_previo_no_hay_nada_que_comparar(self, crudo_temporal, caplog):
        nuevos = pd.DataFrame([{'id_fila': 'x', 'a_o': 2024}])

        with caplog.at_level('INFO'):
            extract._avisar_de_cambios_de_esquema('prueba', nuevos, 'a_o', 'id_fila')

        assert 'columnas nuevas' not in caplog.text
        assert 'ya no publica' not in caplog.text
