"""Pruebas de la capa de almacenamiento.

Se prueban con el almacén local sobre un directorio temporal: sin red, sin bucket
y sin servicios. El almacén de S3 se verifica en el CI, que sí levanta un servidor
S3.
"""
import os

import pandas as pd
import pytest

from pipeline import almacen, config


@pytest.fixture
def crudo_temporal(tmp_path, monkeypatch):
    """Redirige la carpeta del crudo a un directorio temporal."""
    monkeypatch.setattr(config, 'RAW', tmp_path / 'raw')
    return tmp_path / 'raw'


class TestFabrica:
    def test_por_defecto_el_crudo_va_a_disco(self, monkeypatch):
        monkeypatch.delenv('PIPELINE_ALMACEN', raising=False)
        assert almacen.obtener().nombre == 'local'

    def test_se_puede_elegir_el_bucket(self, monkeypatch):
        monkeypatch.setenv('PIPELINE_ALMACEN', 's3')
        assert almacen.obtener().nombre == 's3'

    def test_un_almacen_desconocido_falla_con_mensaje_claro(self, monkeypatch):
        monkeypatch.setenv('PIPELINE_ALMACEN', 'dropbox')
        with pytest.raises(ValueError, match='dropbox'):
            almacen.obtener()


class TestAjustesS3:
    def test_valores_por_defecto(self, monkeypatch):
        for clave in list(os.environ):
            if clave.startswith('PIPELINE_S3_'):
                monkeypatch.delenv(clave, raising=False)

        ajustes = almacen.AjustesS3.desde_entorno()

        assert ajustes.endpoint == 'localhost:9000'
        assert ajustes.bucket == 'pipeline-crudo'
        assert ajustes.usar_ssl is False

    def test_lee_la_configuracion_del_entorno(self, monkeypatch):
        monkeypatch.setenv('PIPELINE_S3_ENDPOINT', 'objetos.miempresa:9000')
        monkeypatch.setenv('PIPELINE_S3_BUCKET', 'crudo-financiero')
        monkeypatch.setenv('PIPELINE_S3_SSL', 'true')

        ajustes = almacen.AjustesS3.desde_entorno()

        assert ajustes.url == 'https://objetos.miempresa:9000'
        assert ajustes.bucket == 'crudo-financiero'

    def test_la_uri_apunta_al_bucket_y_al_prefijo(self):
        ajustes = almacen.AjustesS3(endpoint='localhost:9000', bucket='crudo',
                                    acceso='a', secreto='b')
        assert almacen.AlmacenS3(ajustes).uri('movilizacion_municipio') == (
            's3://crudo/raw/movilizacion_municipio/**/*.parquet'
        )


class TestAlmacenLocal:
    def test_escribe_particionado_y_vuelve_a_leer(self, crudo_temporal):
        tienda = almacen.AlmacenLocal()
        datos = pd.DataFrame([
            {'id_fila': 'a', 'a_o': 2024, 'valor': 1},
            {'id_fila': 'b', 'a_o': 2025, 'valor': 2},
        ])

        tienda.escribir('prueba', datos, 'a_o')

        objetos = tienda.inventario('prueba')
        assert len(objetos) == 2, 'debería haber una partición por año'
        assert any('a_o=2024' in objeto for objeto in objetos)
        assert tienda.tamano('prueba') > 0
        assert sorted(tienda.leer('prueba')['id_fila']) == ['a', 'b']

    def test_reescribir_no_deja_restos_del_año_anterior(self, crudo_temporal):
        tienda = almacen.AlmacenLocal()
        tienda.escribir('prueba', pd.DataFrame([{'id_fila': 'a', 'a_o': 2024}]), 'a_o')
        tienda.escribir('prueba', pd.DataFrame([{'id_fila': 'b', 'a_o': 2025}]), 'a_o')

        leido = tienda.leer('prueba')

        assert list(leido['id_fila']) == ['b']
        assert list(leido['a_o']) == [2025]

    def test_leer_una_fuente_que_no_existe_devuelve_none(self, crudo_temporal):
        assert almacen.AlmacenLocal().leer('no_existe') is None

    def test_la_particion_se_normaliza_a_entero(self, crudo_temporal):
        tienda = almacen.AlmacenLocal()
        tienda.escribir('prueba', pd.DataFrame([{'id_fila': 'a', 'a_o': '2024'}]), 'a_o')

        assert tienda.leer('prueba')['a_o'].iloc[0] == 2024

    def test_las_filas_sin_particion_se_descartan(self, crudo_temporal):
        tienda = almacen.AlmacenLocal()
        datos = pd.DataFrame([
            {'id_fila': 'a', 'a_o': 2024},
            {'id_fila': 'b', 'a_o': None},
        ])

        tienda.escribir('prueba', datos, 'a_o')

        assert list(tienda.leer('prueba')['id_fila']) == ['a']
