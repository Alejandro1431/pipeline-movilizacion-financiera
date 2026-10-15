"""Interfaz de línea de comandos del pipeline.

Cada subcomando es un paso del ELT, y `all` los encadena:

    python -m pipeline extract      # descarga las fuentes a Parquet
    python -m pipeline transform    # construye el staging en DuckDB
    python -m pipeline marts        # modelo dimensional (dbt run + dbt test)
    python -m pipeline quality      # corre las reglas de calidad
    python -m pipeline flujo        # el mismo pipeline, orquestado con Prefect
    python -m pipeline control      # marcas de agua de las cargas incrementales
    python -m pipeline almacen      # dónde vive el crudo y cuánto ocupa
    python -m pipeline perfil       # cifras de negocio del warehouse
    python -m pipeline all          # extract + transform + marts + quality
"""
from __future__ import annotations

import argparse
import contextlib
import logging
import sys

from . import almacen, config, control, extract, flujo, marts, perfil, quality, transform

log = logging.getLogger('pipeline')


def _configurar_log() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s  %(levelname)-7s %(message)s',
        datefmt='%H:%M:%S',
    )
    # En Windows la consola no siempre usa UTF-8 y los acentos salen rotos.
    for salida in (sys.stdout, sys.stderr):
        with contextlib.suppress(AttributeError, ValueError):
            salida.reconfigure(encoding='utf-8', errors='replace')


def _imprimir_tabla(titulo: str, filas: list[tuple[str, str]]) -> None:
    print(f'\n{titulo}')
    print('-' * len(titulo))
    ancho = max((len(clave) for clave, _ in filas), default=0)
    for clave, valor in filas:
        print(f'  {clave.ljust(ancho)}  {valor}')


def comando_extract(args: argparse.Namespace) -> int:
    config.preparar_directorios()
    resumenes = (
        extract.extraer_todas(desde=args.desde, incremental=args.incremental)
        if args.fuente == 'all'
        else [extract.extraer_incremental(args.fuente) if args.incremental
              else extract.extraer(args.fuente, desde=args.desde)]
    )
    _imprimir_tabla('Extracción', [
        (r['fuente'], f"{r['filas']:,} filas en total | {r['modo']}".replace(',', '.'))
        for r in resumenes
    ])
    tamanos = extract.tamano_raw()
    _imprimir_tabla('Tamaño del crudo', [(n, f'{mb:.2f} MB') for n, mb in tamanos.items()])
    return 0


def comando_almacen(_: argparse.Namespace) -> int:
    """Muestra dónde vive el crudo: en disco o en el bucket."""
    tienda = almacen.actual()
    print(f'\nAlmacén del crudo: {tienda.nombre}')
    print('-' * (19 + len(tienda.nombre)))
    for nombre in config.fuentes():
        objetos = tienda.inventario(nombre)
        print(f'  {nombre:26} {len(objetos):>5} objetos   {tienda.tamano(nombre):>8.2f} MB')
    if tienda.nombre == 's3':
        print(f"\n  endpoint: {tienda.ajustes.url}   bucket: {tienda.ajustes.bucket}")
    primera = next(iter(config.fuentes()))
    print(f'\n  uri de ejemplo: {tienda.uri(primera)}')
    return 0


def comando_control(_: argparse.Namespace) -> int:
    filas = control.estado()
    if not filas:
        print('\n  la tabla de control está vacía: todavía no se ha extraído nada')
        return 0
    print('\nControl de cargas incrementales')
    print('-------------------------------')
    for fila in filas:
        marca = fila['ultimo_valor'].strftime('%Y-%m-%d') if fila['ultimo_valor'] else 'sin columna de fecha'
        print(f"  {fila['fuente']:26} desde {marca:22} "
              f"última carga: {fila['filas_ultima_carga']:>7,} filas   total: {fila['filas_totales']:>7,}".replace(',', '.'))
    return 0


def comando_transform(_: argparse.Namespace) -> int:
    resultado = transform.ejecutar()
    _imprimir_tabla('Staging construido', [
        (tabla, f'{filas:,} filas'.replace(',', '.')) for tabla, filas in resultado['tablas'].items()
    ])
    print(f"\n  warehouse: {resultado['warehouse']}")
    vacias = [tabla for tabla, filas in resultado['tablas'].items() if not filas]
    if vacias:
        print(f'  staging vacio en: {", ".join(vacias)}')
        return 1
    return 0


def comando_quality(_: argparse.Namespace) -> int:
    informe = quality.ejecutar()
    print('\nCalidad de datos')
    print('----------------')
    for resultado in informe['resultados']:
        estado = 'OK   ' if resultado['pasa'] else 'FALLA'
        print(f"  {estado} {resultado['tabla']:28} {resultado['regla']} (={resultado['valor']})")
    print(f"\n  {informe['total'] - informe['fallidas']}/{informe['total']} reglas pasan")
    return 0 if informe['veredicto'] == 'pass' else 1


def comando_marts(_: argparse.Namespace) -> int:
    print('\nModelo dimensional (dbt)')
    print('-----------------------')
    resultados = marts.ejecutar()
    fallidos = [comando for comando, codigo in resultados.items() if codigo != 0]
    if fallidos:
        print(f'  dbt fallo en: {", ".join(fallidos)}')
        return 1
    print('  dimensiones, hechos y agregados construidos y probados')
    return 0


def comando_perfil(_: argparse.Namespace) -> int:
    for titulo, columnas, filas in perfil.ejecutar():
        print(f'\n{titulo}')
        print('-' * len(titulo))
        anchos = [
            max(len(str(columnas[i])), *(len(str(fila[i])) for fila in filas)) if filas else len(columnas[i])
            for i in range(len(columnas))
        ]
        print('  ' + '  '.join(str(c).ljust(anchos[i]) for i, c in enumerate(columnas)))
        for fila in filas:
            print('  ' + '  '.join(str(valor).ljust(anchos[i]) for i, valor in enumerate(fila)))
    return 0


def comando_flujo(args: argparse.Namespace) -> int:
    if args.servir:
        print(f'\nSirviendo el flujo con programación "{args.cron}" (Ctrl+C para parar)')
        flujo.servir(cron=args.cron)
        return 0

    print('\nFlujo orquestado con Prefect')
    print('----------------------------')
    resultado = flujo.pipeline(desde=args.desde, incremental=args.incremental)
    print('\n  resumen de la ejecución:')
    for clave, valor in resultado.items():
        print(f'    {clave:22} {valor}')
    return 0


def comando_all(args: argparse.Namespace) -> int:
    codigo = comando_extract(args)
    if codigo:
        return codigo
    codigo = comando_transform(args)
    if codigo:
        return codigo
    codigo = comando_marts(args)
    if codigo:
        return codigo
    return comando_quality(args)


def construir_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='pipeline', description='ELT de movilización financiera en Colombia')
    sub = parser.add_subparsers(dest='comando', required=True)

    p_extract = sub.add_parser('extract', help='descarga las fuentes a Parquet')
    p_extract.add_argument('--fuente', default='all', choices=['all', *config.fuentes().keys()])
    p_extract.add_argument('--desde', default=None, help='fecha ISO para carga incremental (YYYY-MM-DD)')
    p_extract.add_argument('--incremental', action='store_true',
                           help='usa la marca de agua del warehouse en vez de recargar todo')
    p_extract.set_defaults(func=comando_extract)

    sub.add_parser('control', help='muestra las marcas de agua de las cargas').set_defaults(func=comando_control)
    sub.add_parser('almacen', help='muestra dónde vive el crudo (disco o bucket)').set_defaults(func=comando_almacen)

    sub.add_parser('transform', help='construye el staging en DuckDB').set_defaults(func=comando_transform)
    sub.add_parser('marts', help='construye el modelo dimensional con dbt').set_defaults(func=comando_marts)
    sub.add_parser('quality', help='corre las reglas de calidad').set_defaults(func=comando_quality)
    sub.add_parser('perfil', help='perfila el contenido del warehouse').set_defaults(func=comando_perfil)

    p_all = sub.add_parser('all', help='extract + transform + marts + quality')
    p_all.add_argument('--fuente', default='all', choices=['all', *config.fuentes().keys()])
    p_all.add_argument('--desde', default=None)
    p_all.add_argument('--incremental', action='store_true')
    p_all.set_defaults(func=comando_all)

    p_flujo = sub.add_parser('flujo', help='ejecuta el pipeline orquestado con Prefect')
    p_flujo.add_argument('--desde', default=None, help='fecha ISO para carga incremental')
    p_flujo.add_argument('--incremental', action='store_true', help='usa las marcas de agua')
    p_flujo.add_argument('--servir', action='store_true', help='deja el flujo servido con una programación')
    p_flujo.add_argument('--cron', default='0 6 * * 1', help='expresión cron para --servir')
    p_flujo.set_defaults(func=comando_flujo)

    return parser


def main(argv: list[str] | None = None) -> int:
    _configurar_log()
    args = construir_parser().parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
