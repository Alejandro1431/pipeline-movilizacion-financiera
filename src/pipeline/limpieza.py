"""Funciones puras de limpieza.

Van aparte de `transform.py` para poder probarlas sin pandas, sin DuckDB y sin
conectarse a nada. Son las que arreglan las dos trampas de la fuente sucia:
valores monetarios como texto y geometría anidada.
"""
from __future__ import annotations

import re

_SOLO_NUMERO = re.compile(r'[^0-9.,\-]')


def parsear_moneda(valor: object) -> float | None:
    """Convierte importes en texto a número.

    Verificado contra las 2.982 filas del origen: todas usan formato
    estadounidense, con coma de miles y punto decimal ('$    7,000,000,000.00'),
    repartidas en 6 familias según la magnitud. Ninguna trae paréntesis, signo
    negativo ni letras.

    La rama que interpreta el formato europeo ('1.234,56') no la ejercita este
    dataset: está cubierta por pruebas unitarias, pero no ha visto datos reales.
    """
    if valor is None:
        return None
    if isinstance(valor, (int, float)):
        return float(valor)

    texto = _SOLO_NUMERO.sub('', str(valor)).strip()
    if not texto or texto in {'-', '.', ','}:
        return None

    ultima_coma = texto.rfind(',')
    ultimo_punto = texto.rfind('.')

    # Si la última coma va después del último punto, el número viene en formato
    # europeo (1.234.567,89): el punto separa miles y la coma es decimal.
    europeo = ultima_coma > ultimo_punto
    texto = (
        texto.replace('.', '').replace(',', '.') if europeo
        else texto.replace(',', '')
    )

    try:
        return float(texto)
    except ValueError:
        return None


def extraer_coordenadas(valor: object) -> tuple[float | None, float | None]:
    """Saca longitud y latitud de un objeto GeoJSON anidado.

    Espera {'type': 'Point', 'coordinates': [lon, lat]}. Devuelve (None, None)
    si la forma no es la esperada, en lugar de reventar el pipeline.
    """
    if not isinstance(valor, dict):
        return None, None

    coordenadas = valor.get('coordinates')
    if not isinstance(coordenadas, (list, tuple)) or len(coordenadas) < 2:
        return None, None

    try:
        return float(coordenadas[0]), float(coordenadas[1])
    except (TypeError, ValueError):
        return None, None


def limpiar_texto(valor: object) -> str | None:
    """Normaliza espacios y deja None cuando el valor viene vacío."""
    if valor is None:
        return None
    texto = ' '.join(str(valor).split())
    return texto or None


def codigo_con_ceros(valor: object, largo: int) -> str | None:
    """Rellena códigos con ceros a la izquierda (Socrata los publica sin ellos)."""
    texto = limpiar_texto(valor)
    if texto is None:
        return None
    solo_digitos = re.sub(r'\D', '', texto)
    if not solo_digitos:
        return None
    return solo_digitos.zfill(largo)
