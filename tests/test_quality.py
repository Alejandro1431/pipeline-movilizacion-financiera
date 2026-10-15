"""Pruebas de las reglas de calidad.

La semántica de una regla (esperado, mínimo, máximo) se prueba contra una base
DuckDB en memoria, sin tocar el warehouse del proyecto. Lo que se comprueba es
que un mínimo es un suelo y no una igualdad, que un máximo es un techo, y que una
consulta rota marca la regla como fallida en lugar de tumbar el informe.
"""
import duckdb
import pytest

from pipeline import config, quality


@pytest.fixture
def conexion():
    with duckdb.connect() as base:
        yield base


class TestEvaluar:
    def test_sin_violaciones_pasa(self, conexion):
        assert quality.Regla('t', 'sin filas malas', 'select 0').evaluar(conexion) == (True, 0)

    def test_con_violaciones_falla(self, conexion):
        assert quality.Regla('t', 'sin filas malas', 'select 3').evaluar(conexion) == (False, 3)

    def test_el_minimo_es_un_suelo_no_una_igualdad(self, conexion):
        regla = quality.Regla('t', 'periodos suficientes', 'select 213', minimo=150)
        assert regla.evaluar(conexion) == (True, 213)

    def test_por_debajo_del_minimo_falla(self, conexion):
        regla = quality.Regla('t', 'periodos suficientes', 'select 149', minimo=150)
        assert regla.evaluar(conexion) == (False, 149)

    def test_el_maximo_es_un_techo_inclusive(self, conexion):
        regla = quality.Regla('t', 'marginales', 'select 200', maximo=200)
        assert regla.evaluar(conexion) == (True, 200)

    def test_por_encima_del_maximo_falla(self, conexion):
        regla = quality.Regla('t', 'marginales', 'select 201', maximo=200)
        assert regla.evaluar(conexion) == (False, 201)

    def test_un_nulo_cuenta_como_cero(self, conexion):
        assert quality.Regla('t', 'sin filas malas', 'select null').evaluar(conexion) == (True, 0)


class TestCatalogo:
    def test_hay_quince_reglas(self):
        assert len(quality.reglas(2026)) == 15

    def test_ninguna_tabla_repite_el_nombre_de_una_regla(self):
        # Dos tablas distintas pueden llamar igual a su regla de negativos; lo que
        # no puede repetirse es el par (tabla, regla), que es la clave del informe.
        parejas = [(regla.tabla, regla.nombre) for regla in quality.reglas(2026)]
        assert len(set(parejas)) == len(parejas)

    def test_el_anio_maximo_entra_en_la_consulta(self):
        regla = next(r for r in quality.reglas(2030) if 'rango esperado' in r.nombre)
        assert '2030' in regla.consulta

    def test_cubren_staging_crudo_y_control(self):
        tablas = {regla.tabla for regla in quality.reglas(2026)}
        assert tablas == {
            'movilizacion_municipio', 'movilizacion_intermediario',
            'desembolsos_redescuento', 'raw', 'control',
        }


class TestEjecutar:
    def test_una_regla_rota_no_tumba_el_informe(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, 'DATA', tmp_path)
        monkeypatch.setattr(config, 'WAREHOUSE', tmp_path / 'prueba.duckdb')
        monkeypatch.setattr(quality, 'reglas', lambda anio: [
            quality.Regla('t', 'rota', 'select * from una_tabla_que_no_existe'),
            quality.Regla('t', 'buena', 'select 0'),
        ])

        informe = quality.ejecutar(2026, guardar=False)

        assert informe['total'] == 2
        assert informe['fallidas'] == 1
        assert informe['veredicto'] == 'fail'
        rota = next(r for r in informe['resultados'] if r['regla'] == 'rota')
        assert rota == {'tabla': 't', 'regla': 'rota', 'valor': -1, 'pasa': False}

    def test_todo_en_verde_da_veredicto_pass(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, 'DATA', tmp_path)
        monkeypatch.setattr(config, 'WAREHOUSE', tmp_path / 'prueba.duckdb')
        monkeypatch.setattr(quality, 'reglas', lambda anio: [quality.Regla('t', 'buena', 'select 0')])

        informe = quality.ejecutar(2026, guardar=False)

        assert informe['veredicto'] == 'pass'
        assert informe['fallidas'] == 0

    def test_guarda_el_informe_en_disco_cuando_se_le_pide(self, tmp_path, monkeypatch):
        monkeypatch.setattr(config, 'DATA', tmp_path)
        monkeypatch.setattr(config, 'WAREHOUSE', tmp_path / 'prueba.duckdb')
        monkeypatch.setattr(quality, 'reglas', lambda anio: [quality.Regla('t', 'buena', 'select 0')])

        quality.ejecutar(2026, guardar=True)

        assert (tmp_path / 'calidad.json').exists()
