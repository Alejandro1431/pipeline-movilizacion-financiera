"""Pruebas de la construcción del staging.

La limpieza de la fuente sucia se prueba con un crudo de juguete registrado en una
base DuckDB en memoria. Lleva las dos trampas del origen (el dinero como texto y
la geografía anidada) y dos filas que tienen que quedar fuera.
"""
import duckdb
import pandas as pd
import pytest

from pipeline import almacen, config, transform

FILAS = [
    # Dos filas buenas: una con coordenadas y otra sin ellas.
    {'ano': '2024', 'departamento': '  Antioquia ', 'sector': 'Vivienda',
     'valor_desembolsado': '$    7,000,000,000.00',
     'ubicacion_longitud_latitud': {'type': 'Point', 'coordinates': [-75.5, 6.2]}},
    {'ano': '2024', 'departamento': 'Caldas', 'sector': 'Salud',
     'valor_desembolsado': '$ 1,000.00', 'ubicacion_longitud_latitud': None},
    # Dos filas malas: una sin año y otra con un valor que no es un número.
    {'ano': None, 'departamento': 'Caquetá', 'sector': 'Agua',
     'valor_desembolsado': '$ 5,000.00', 'ubicacion_longitud_latitud': None},
    {'ano': '2024', 'departamento': 'Nariño', 'sector': 'Energía',
     'valor_desembolsado': 'sin dato', 'ubicacion_longitud_latitud': None},
]


@pytest.fixture
def staging_temporal(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'DATA', tmp_path)
    monkeypatch.setattr(config, 'RAW', tmp_path / 'raw')
    monkeypatch.setattr(config, 'STAGING', tmp_path / 'staging')
    monkeypatch.setattr(config, 'WAREHOUSE', tmp_path / 'prueba.duckdb')
    return tmp_path / 'staging'


@pytest.fixture
def conexion():
    """Base en memoria con el crudo de juguete ya expuesto como vista raw."""
    with duckdb.connect() as base:
        base.execute('create schema raw')
        base.execute('create schema staging')
        base.register('crudo', pd.DataFrame(FILAS))
        base.execute('create view raw.desembolsos_redescuento as select * from crudo')
        yield base


class TestLimpiarDesembolsos:
    def test_descarta_las_filas_sin_anio_o_sin_valor(self, conexion, staging_temporal):
        transform.limpiar_desembolsos(conexion)

        filas = conexion.execute(
            'select count(*) from staging.desembolsos_redescuento'
        ).fetchone()[0]

        assert filas == 2

    def test_parsea_el_dinero_y_normaliza_el_texto(self, conexion, staging_temporal):
        transform.limpiar_desembolsos(conexion)

        anio, departamento, valor = conexion.execute(
            "select anio, departamento, valor_desembolsado "
            'from staging.desembolsos_redescuento where departamento = ?', ['Antioquia']
        ).fetchone()

        assert anio == 2024
        assert departamento == 'Antioquia'
        assert valor == 7_000_000_000.0

    def test_saca_las_coordenadas_del_geojson(self, conexion, staging_temporal):
        transform.limpiar_desembolsos(conexion)

        longitud, latitud = conexion.execute(
            "select longitud, latitud from staging.desembolsos_redescuento "
            "where departamento = 'Antioquia'"
        ).fetchone()

        assert (longitud, latitud) == (-75.5, 6.2)

    def test_deja_el_parquet_en_la_carpeta_de_staging(self, conexion, staging_temporal):
        transform.limpiar_desembolsos(conexion)

        assert (staging_temporal / 'desembolsos_redescuento.parquet').exists()


class TestAplicarSql:
    def test_deriva_el_nombre_de_la_tabla_del_archivo(self, tmp_path, monkeypatch):
        (tmp_path / 'staging_prueba.sql').write_text(
            'create table staging.prueba as select 1 as x', encoding='utf-8'
        )
        monkeypatch.setattr(config, 'SQL_DIR', tmp_path)

        with duckdb.connect() as base:
            base.execute('create schema staging')

            assert transform.aplicar_sql(base) == ['prueba']
            assert base.execute('select x from staging.prueba').fetchone()[0] == 1

    def test_ignora_los_sql_que_no_son_de_staging(self, tmp_path, monkeypatch):
        (tmp_path / 'otra_cosa.sql').write_text('select 1', encoding='utf-8')
        monkeypatch.setattr(config, 'SQL_DIR', tmp_path)

        with duckdb.connect() as base:
            assert transform.aplicar_sql(base) == []


class TestVistasRaw:
    def test_expone_una_vista_por_fuente(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, 'DATA', tmp_path)
        monkeypatch.setattr(config, 'RAW', tmp_path / 'raw')
        monkeypatch.setattr(config, 'STAGING', tmp_path / 'staging')
        monkeypatch.delenv('PIPELINE_ALMACEN', raising=False)

        tienda = almacen.AlmacenLocal()
        for nombre in config.fuentes():
            tienda.escribir(nombre, pd.DataFrame([{'id_fila': 'a', 'a_o': 2024}]), 'a_o')

        with duckdb.connect() as base:
            base.execute('create schema raw')
            transform.vistas_raw(base)

            for nombre in config.fuentes():
                assert base.execute(f'select count(*) from raw.{nombre}').fetchone()[0] == 1
