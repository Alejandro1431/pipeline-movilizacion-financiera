"""Pruebas de las funciones de limpieza.

Son funciones puras: se prueban sin red, sin pandas y sin base de datos.
"""
import pytest

from pipeline.limpieza import (
    codigo_con_ceros,
    extraer_coordenadas,
    limpiar_texto,
    parsear_moneda,
)


class TestParsearMoneda:
    def test_formato_con_simbolo_y_coma_de_miles(self):
        assert parsear_moneda('$    7,000,000,000.00') == 7_000_000_000.00

    def test_formato_con_punto_de_miles_y_coma_decimal(self):
        assert parsear_moneda('1.234.567,89') == 1_234_567.89

    def test_negativo_entre_parentesis_no_aplica_coma_decimal(self):
        assert parsear_moneda('-1,500.50') == -1500.50

    def test_numero_ya_numerico(self):
        assert parsear_moneda(174950000) == 174950000.0

    def test_vacios_y_no_numericos_devuelven_none(self):
        assert parsear_moneda(None) is None
        assert parsear_moneda('') is None
        assert parsear_moneda('   ') is None
        assert parsear_moneda('N/A') is None
        assert parsear_moneda('$') is None


class TestExtraerCoordenadas:
    def test_punto_geojson_valido(self):
        assert extraer_coordenadas({'type': 'Point', 'coordinates': [-74.1, 4.65]}) == (-74.1, 4.65)

    def test_formas_invalidas_no_revientan(self):
        assert extraer_coordenadas(None) == (None, None)
        assert extraer_coordenadas('POINT(-74 4)') == (None, None)
        assert extraer_coordenadas({'coordinates': []}) == (None, None)
        assert extraer_coordenadas({'coordinates': ['a', 'b']}) == (None, None)


class TestCodigoConCeros:
    def test_rellena_a_la_izquierda(self):
        assert codigo_con_ceros(5, 2) == '05'
        assert codigo_con_ceros('5', 5) == '00005'

    def test_respeta_codigos_completos(self):
        assert codigo_con_ceros(11001, 5) == '11001'
        assert codigo_con_ceros('08', 2) == '08'

    def test_sin_digitos_devuelve_none(self):
        assert codigo_con_ceros(None, 2) is None
        assert codigo_con_ceros('sin dato', 2) is None


class TestLimpiarTexto:
    def test_colapsa_espacios(self):
        assert limpiar_texto('  Bogotá,   D.C. ') == 'Bogotá, D.C.'

    def test_vacio_es_none(self):
        assert limpiar_texto('   ') is None
        assert limpiar_texto(None) is None


class TestValoresRealesDelOrigen:
    """Casos copiados literalmente del dataset bu9y-ytgi.

    No son formatos inventados: son las 6 familias de formato que existen de
    verdad en las 2.982 filas publicadas, más un par de valores concretos.
    """

    @pytest.mark.parametrize(
        ('crudo', 'esperado'),
        [
            ('$    1,000,000.00', 1_000_000.00),
            ('$    100,000,000.00', 100_000_000.00),
            ('$    1,000,000,000.00', 1_000_000_000.00),
            ('$    10,000,000,000.00', 10_000_000_000.00),
            ('$    100,000,000,000.00', 100_000_000_000.00),
            ('$    5,999,350,000.00', 5_999_350_000.00),
            ('$    208,670,000,000.00', 208_670_000_000.00),
        ],
    )
    def test_formatos_que_existen_en_el_origen(self, crudo, esperado):
        assert parsear_moneda(crudo) == esperado
