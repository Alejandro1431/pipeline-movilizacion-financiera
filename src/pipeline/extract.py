"""Extracción: API Socrata -> Parquet crudo particionado por año.

Dos modos de carga:

* **Completo**: borra la partición y la reescribe. Es idempotente.
* **Incremental**: pide al origen solo lo publicado desde la última marca de agua
  y lo fusiona con el crudo existente sin duplicar filas. La marca vive en la
  tabla de control del warehouse.

Toda extracción pide el identificador de fila del origen (`:id`, publicado como
`id_fila`), que es lo que permite fusionar sin ambigüedad y comprobar después que
no quedaron filas repetidas.
"""
from __future__ import annotations

import datetime as dt
import logging

import duckdb
import pandas as pd

from . import almacen, config, control, crudo
from .socrata import TAMANO_PAGINA, ClienteSocrata

log = logging.getLogger(__name__)


def _cliente(nombre: str) -> ClienteSocrata:
    definicion = config.fuente(nombre)
    return ClienteSocrata(dominio=definicion['dominio'], dataset=definicion['dataset'])


def _seleccion(cliente: ClienteSocrata, definicion: dict) -> str:
    """Lista de columnas a pedir, con el identificador de fila renombrado.

    Socrata no acepta `$select=:id,*` (el comodín no se puede mezclar con el
    identificador), así que hay que enumerar las columnas del origen.
    """
    campos = cliente.columnas()
    if not campos:
        raise RuntimeError(f"{definicion['dataset']}: el origen no publicó su esquema")
    return ', '.join([f":id as {definicion['columna_id']}", *campos])


def _columnas_previas(nombre: str, particion: str) -> set[str]:
    """Columnas del crudo ya almacenado, sin contar la de partición.

    Se consulta el esquema con DuckDB en lugar de leer un archivo concreto: así
    funciona igual si el crudo está en disco o en un bucket. La columna de
    partición no vive dentro del archivo (va codificada en la ruta), así que se
    excluye para no confundirla con una columna nueva.
    """
    tienda = almacen.actual()
    if not tienda.inventario(nombre):
        return set()

    with duckdb.connect() as conexion:
        tienda.configurar(conexion)
        try:
            filas = conexion.execute(
                f"describe select * from read_parquet('{tienda.uri(nombre)}', "
                f'hive_partitioning = true, union_by_name = true)'
            ).fetchall()
        except Exception as error:  # noqa: BLE001
            log.warning('%s: no pude leer el esquema anterior (%s)', nombre, error)
            return set()

    return {fila[0] for fila in filas} - {particion}


def _descargar(nombre: str, where: str | None = None, tamano_pagina: int = TAMANO_PAGINA) -> pd.DataFrame:
    """Descarga una fuente (o la ventana que indique `where`) y devuelve el crudo."""
    definicion = config.fuente(nombre)
    cliente = _cliente(nombre)
    seleccion = _seleccion(cliente, definicion)

    total = cliente.contar(where=where)
    log.info(
        '%s: %s filas por descargar%s',
        nombre,
        f'{total:,}'.replace(',', '.'),
        f' con filtro {where}' if where else '',
    )
    if total == 0:
        return pd.DataFrame()

    marcos: list[pd.DataFrame] = []
    for pagina, filas in cliente.paginar(total, tamano=tamano_pagina, where=where, columnas=seleccion):
        if pagina % 5 == 1 or pagina * tamano_pagina >= total:
            log.info('  pagina %s: %s filas acumuladas', pagina, sum(len(m) for m in marcos) + len(filas))
        marcos.append(pd.DataFrame(filas))

    datos = pd.concat(marcos, ignore_index=True)
    datos.columns = [str(columna).strip().lower() for columna in datos.columns]
    return datos


def _leer_crudo(nombre: str) -> pd.DataFrame | None:
    """Lee el crudo ya almacenado de una fuente, si existe."""
    return almacen.actual().leer(nombre)


def _escribir_crudo(nombre: str, datos: pd.DataFrame, particion: str) -> str:
    """Escribe el crudo en el almacén activo, reemplazando lo que hubiera."""
    return almacen.actual().escribir(nombre, datos, particion)


def _ultima_fecha(datos: pd.DataFrame, columna_fecha: str | None) -> dt.datetime | None:
    """Valor máximo de la columna de fecha, que es la próxima marca de agua."""
    if not columna_fecha or datos is None or datos.empty or columna_fecha not in datos.columns:
        return None
    valores = pd.to_datetime(datos[columna_fecha], errors='coerce').dropna()
    if valores.empty:
        return None
    return valores.max().to_pydatetime()


def _miles(numero: int) -> str:
    return f'{numero:,}'.replace(',', '.')


def _avisar_de_cambios_de_esquema(nombre: str, datos: pd.DataFrame, particion: str, columna_id: str) -> None:
    """Compara las columnas descargadas con las del crudo anterior.

    Es lo que convierte una carga silenciosa en un aviso: si el publicador añade,
    renombra o retira una columna, queda en el log en lugar de pasar inadvertido.
    """
    previas = _columnas_previas(nombre, particion)
    if not previas:
        return

    actuales = set(datos.columns) - {particion}
    nuevas = actuales - previas
    retiradas = previas - actuales

    if nuevas:
        log.info('%s: columnas nuevas en el origen: %s', nombre, ', '.join(sorted(nuevas)))
    if retiradas:
        log.warning('%s: el origen ya no publica estas columnas: %s', nombre, ', '.join(sorted(retiradas)))
    if retiradas and columna_id not in actuales:
        log.warning('%s: desaparecio el identificador de fila; la carga incremental no sera fiable', nombre)


def extraer(nombre: str, tamano_pagina: int = TAMANO_PAGINA, desde: str | None = None) -> dict:
    """Refresco completo de una fuente, con opción de filtrar desde una fecha."""
    definicion = config.fuente(nombre)
    columna_fecha = definicion.get('columna_fecha')
    where = f"{columna_fecha} >= '{desde}'" if (desde and columna_fecha) else None

    datos = _descargar(nombre, where=where, tamano_pagina=tamano_pagina)
    if datos.empty:
        log.warning('%s: el origen no devolvio filas, no se toca la particion', nombre)
        return {'fuente': nombre, 'filas': 0, 'modo': 'completo', 'destino': None}

    _avisar_de_cambios_de_esquema(nombre, datos, definicion['particion'], definicion['columna_id'])
    destino = _escribir_crudo(nombre, datos, definicion['particion'])
    control.guardar_marca(nombre, columna_fecha, _ultima_fecha(datos, columna_fecha), len(datos), len(datos))
    log.info('%s: %s filas escritas en %s', nombre, _miles(len(datos)), destino)

    return {'fuente': nombre, 'filas': len(datos), 'filas_nuevas': len(datos),
            'modo': 'completo', 'destino': str(destino)}


def extraer_incremental(nombre: str, tamano_pagina: int = TAMANO_PAGINA) -> dict:
    """Trae solo lo publicado desde la última marca de agua y lo fusiona."""
    definicion = config.fuente(nombre)
    columna_fecha = definicion.get('columna_fecha')
    columna_id = definicion['columna_id']

    # Sin columna de fecha fiable no hay marca de agua posible: se recarga entera.
    if not columna_fecha:
        resumen = extraer(nombre, tamano_pagina)
        resumen['modo'] = 'completo (el origen no tiene columna de fecha)'
        return resumen

    marca = control.leer_marca(nombre)
    if marca is None:
        resumen = extraer(nombre, tamano_pagina)
        resumen['modo'] = 'completo (primera carga)'
        return resumen

    where = f"{columna_fecha} >= '{marca:%Y-%m-%d}'"
    nuevos = _descargar(nombre, where=where, tamano_pagina=tamano_pagina)
    existente = _leer_crudo(nombre)

    if nuevos.empty:
        log.info('%s: no hay nada nuevo desde %s', nombre, f'{marca:%Y-%m-%d}')
        return {'fuente': nombre, 'filas': len(existente) if existente is not None else 0,
                'filas_nuevas': 0, 'modo': f'incremental (sin novedades desde {marca:%Y-%m-%d})',
                'destino': str(config.RAW / nombre)}

    fusionado = crudo.fusionar(existente, nuevos, [columna_id])
    destino = _escribir_crudo(nombre, fusionado, definicion['particion'])

    marca_nueva = _ultima_fecha(nuevos, columna_fecha) or marca
    control.guardar_marca(nombre, columna_fecha, marca_nueva, len(nuevos), len(fusionado))

    log.info(
        '%s: %s filas descargadas, %s en total tras fusionar',
        nombre, _miles(len(nuevos)), _miles(len(fusionado)),
    )
    return {'fuente': nombre, 'filas': len(fusionado), 'filas_nuevas': len(nuevos),
            'modo': f'incremental (desde {marca:%Y-%m-%d})', 'destino': str(destino)}


def extraer_todas(desde: str | None = None, incremental: bool = False) -> list[dict]:
    """Extrae todas las fuentes, en modo completo o incremental."""
    config.preparar_directorios()
    resumenes: list[dict] = []
    for nombre in config.fuentes():
        if incremental:
            resumenes.append(extraer_incremental(nombre))
        else:
            resumenes.append(extraer(nombre, desde=desde))
    return resumenes


def tamano_raw() -> dict[str, float]:
    """Tamaño en MB del crudo de cada fuente, en el almacén activo."""
    tienda = almacen.actual()
    return {nombre: tienda.tamano(nombre) for nombre in config.fuentes()}
