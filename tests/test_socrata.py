"""Pruebas del cliente Socrata, con una sesión falsa (sin red)."""
import pytest

from pipeline.socrata import ClienteSocrata, ErrorSocrata


class RespuestaFalsa:
    def __init__(self, datos, status_code=200):
        self._datos = datos
        self.status_code = status_code

    def json(self):
        return self._datos

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class SesionFalsa:
    """Devuelve respuestas en orden y guarda los parámetros recibidos."""

    def __init__(self, respuestas):
        self.respuestas = list(respuestas)
        self.llamadas = []

    def get(self, url, params=None, timeout=None):  # noqa: ARG002
        self.llamadas.append(dict(params or {}))
        return self.respuestas.pop(0)


def cliente(sesion, **extra):
    return ClienteSocrata(dominio='www.datos.gov.co', dataset='abc1-2345',
                          sesion=sesion, pausa_base=0, **extra)


class TestContar:
    def test_lee_el_conteo_del_origen(self):
        sesion = SesionFalsa([RespuestaFalsa([{'count': '12350'}])])
        assert cliente(sesion).contar() == 12350

    def test_sin_filas_devuelve_cero(self):
        sesion = SesionFalsa([RespuestaFalsa([])])
        assert cliente(sesion).contar() == 0


class TestPaginar:
    def test_recorre_todas_las_paginas_con_desplazamiento(self):
        sesion = SesionFalsa([
            RespuestaFalsa([{'id': 1}, {'id': 2}]),
            RespuestaFalsa([{'id': 3}]),
        ])
        paginas = list(cliente(sesion).paginar(total=3, tamano=2))

        assert [numero for numero, _ in paginas] == [1, 2]
        assert [len(filas) for _, filas in paginas] == [2, 1]
        assert sesion.llamadas[0]['$offset'] == '0'
        assert sesion.llamadas[1]['$offset'] == '2'
        # El orden explícito es lo que evita filas repetidas entre páginas.
        assert all(llamada['$order'] == ':id' for llamada in sesion.llamadas)

    def test_la_ultima_pagina_no_pide_mas_de_lo_que_falta(self):
        sesion = SesionFalsa([RespuestaFalsa([{'id': 1}, {'id': 2}])])
        list(cliente(sesion).paginar(total=2, tamano=50_000))
        assert sesion.llamadas[0]['$limit'] == '2'

    def test_una_pagina_vacia_corta_el_recorrido(self):
        sesion = SesionFalsa([RespuestaFalsa([])])
        assert list(cliente(sesion).paginar(total=10, tamano=5)) == []

    def test_propaga_el_filtro_incremental(self):
        sesion = SesionFalsa([RespuestaFalsa([{'id': 1}])])
        list(cliente(sesion).paginar(total=1, where="fecha >= '2024-01-01'"))
        assert sesion.llamadas[0]['$where'] == "fecha >= '2024-01-01'"


class TestReintentos:
    def test_reintenta_ante_un_error_del_servidor(self):
        sesion = SesionFalsa([
            RespuestaFalsa([], status_code=503),
            RespuestaFalsa([{'count': '7'}]),
        ])
        assert cliente(sesion).contar() == 7
        assert len(sesion.llamadas) == 2

    def test_se_rinde_tras_agotar_los_intentos(self):
        sesion = SesionFalsa([RespuestaFalsa([], status_code=503) for _ in range(3)])
        with pytest.raises(ErrorSocrata):
            cliente(sesion, reintentos=3).contar()
        assert len(sesion.llamadas) == 3
