"""Cliente mínimo para la API Socrata (datos abiertos).

Solo cubre lo que necesita el pipeline: contar filas y recorrer el dataset por
páginas. La paginación va con `$order=:id` a propósito: sin un orden estable,
Socrata puede repetir o saltarse filas entre páginas.
"""
from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass, field

import requests

CODIGOS_REINTENTABLES = {429, 500, 502, 503, 504}
TAMANO_PAGINA = 50_000  # máximo que acepta Socrata


class ErrorSocrata(RuntimeError):
    """Fallo irrecuperable al hablar con la API."""


@dataclass
class ClienteSocrata:
    dominio: str
    dataset: str
    timeout: int = 90
    reintentos: int = 4
    pausa_base: float = 1.5
    sesion: requests.Session = field(default_factory=requests.Session)

    @property
    def url(self) -> str:
        return f'https://{self.dominio}/resource/{self.dataset}.json'

    def _pedir(self, parametros: dict, url: str | None = None) -> list[dict] | dict:
        """Una petición con reintentos y espera exponencial."""
        destino = url or self.url
        ultimo_error: Exception | None = None

        for intento in range(1, self.reintentos + 1):
            try:
                respuesta = self.sesion.get(destino, params=parametros, timeout=self.timeout)
                if respuesta.status_code in CODIGOS_REINTENTABLES:
                    raise ErrorSocrata(f'HTTP {respuesta.status_code} en {destino}')
                respuesta.raise_for_status()
                return respuesta.json()
            except Exception as error:  # noqa: BLE001
                ultimo_error = error
                if intento == self.reintentos:
                    break
                time.sleep(self.pausa_base * (2 ** (intento - 1)))

        raise ErrorSocrata(f'{self.dataset}: fallaron {self.reintentos} intentos ({ultimo_error})')

    def columnas(self) -> list[str]:
        """Nombres de campo que publica el dataset, leídos de su propio esquema.

        Se leen del origen en lugar de escribirlos a mano por dos razones:
        para poder pedir el identificador de fila junto al resto de columnas
        (Socrata no acepta el comodín `*` en esa combinación) y para que un
        cambio de esquema del publicador se refleje sin tocar el código.
        """
        metadatos = self._pedir({}, url=f'https://{self.dominio}/api/views/{self.dataset}.json')
        columnas = metadatos.get('columns', []) if isinstance(metadatos, dict) else []
        return [columna['fieldName'] for columna in columnas if columna.get('fieldName')]

    def contar(self, where: str | None = None) -> int:
        parametros: dict[str, str] = {'$select': 'count(*)'}
        if where:
            parametros['$where'] = where
        filas = self._pedir(parametros)
        if not filas:
            return 0
        return int(next(iter(filas[0].values())))

    def paginar(
        self,
        total: int,
        tamano: int = TAMANO_PAGINA,
        where: str | None = None,
        columnas: str | None = None,
    ) -> Iterator[tuple[int, list[dict]]]:
        """Devuelve (numero_de_pagina, filas) hasta agotar el dataset."""
        desplazamiento = 0
        pagina = 0

        while desplazamiento < total:
            pagina += 1
            parametros: dict[str, str] = {
                '$limit': str(min(tamano, total - desplazamiento)),
                '$offset': str(desplazamiento),
                '$order': ':id',
            }
            if where:
                parametros['$where'] = where
            if columnas:
                parametros['$select'] = columnas

            filas = self._pedir(parametros)
            if not filas:
                break

            yield pagina, filas
            desplazamiento += len(filas)
