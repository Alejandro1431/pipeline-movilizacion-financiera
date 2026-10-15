"""Capa de almacenamiento del crudo: en disco o en un bucket S3 compatible.

El pipeline no escribe el Parquet directamente: lo hace a través de esta
interfaz. Así el mismo código funciona con el crudo en disco o en un bucket
(un servidor S3 de pruebas en desarrollo, un bucket real en producción: el código
solo ve un endpoint y unas credenciales).

Se elige con la variable de entorno `PIPELINE_ALMACEN` (`local` o `s3`) y los
ajustes de S3 con:

    PIPELINE_S3_ENDPOINT   host:puerto del servicio (por defecto localhost:9000)
    PIPELINE_S3_BUCKET     nombre del bucket (por defecto pipeline-crudo)
    PIPELINE_S3_ACCESO     clave de acceso
    PIPELINE_S3_SECRETO    clave secreta
    PIPELINE_S3_SSL        true si el endpoint usa HTTPS

Para probarlo en local, `moto` levanta un servidor S3 en Python:

    pip install -r requirements-dev.txt
    moto_server -p 9000
"""
from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import duckdb
import pandas as pd

from . import config

log = logging.getLogger(__name__)

PREFIJO_CRUDO = 'raw'


class Almacen(Protocol):
    """Lo que el pipeline necesita de un almacén de crudo."""

    nombre: str

    def escribir(self, fuente: str, datos: pd.DataFrame, particion: str) -> str: ...

    def leer(self, fuente: str) -> pd.DataFrame | None: ...

    def uri(self, fuente: str) -> str: ...

    def configurar(self, conexion: duckdb.DuckDBPyConnection) -> None: ...

    def inventario(self, fuente: str) -> list[str]: ...

    def tamano(self, fuente: str) -> float:
        """Tamaño del crudo de una fuente, en MB."""
        ...


def _preparar(datos: pd.DataFrame, particion: str) -> pd.DataFrame:
    """Normaliza el marco antes de escribirlo: nombres y tipo de la partición."""
    datos = datos.copy()
    datos.columns = [str(columna).strip().lower() for columna in datos.columns]
    datos[particion] = pd.to_numeric(datos[particion], errors='coerce')
    datos = datos.dropna(subset=[particion])
    datos[particion] = datos[particion].astype('int64')
    return datos


# --------------------------------------------------------------------- disco

class AlmacenLocal:
    """Crudo en disco, particionado por año. Es el modo por defecto."""

    nombre = 'local'

    def _ruta(self, fuente: str) -> Path:
        return config.RAW / fuente

    def uri(self, fuente: str) -> str:
        return (self._ruta(fuente) / '**' / '*.parquet').as_posix()

    def configurar(self, conexion: duckdb.DuckDBPyConnection) -> None:
        """En local no hay nada que configurar."""
        return None

    def escribir(self, fuente: str, datos: pd.DataFrame, particion: str) -> str:
        destino = self._ruta(fuente)
        if destino.exists():
            shutil.rmtree(destino)
        destino.mkdir(parents=True, exist_ok=True)
        _preparar(datos, particion).to_parquet(destino, partition_cols=[particion], index=False)
        return str(destino)

    def leer(self, fuente: str) -> pd.DataFrame | None:
        if not self.inventario(fuente):
            return None
        with duckdb.connect() as conexion:
            return conexion.execute(
                f"select * from read_parquet('{self.uri(fuente)}', "
                f'hive_partitioning = true, union_by_name = true)'
            ).df()

    def inventario(self, fuente: str) -> list[str]:
        ruta = self._ruta(fuente)
        if not ruta.exists():
            return []
        return sorted(
            archivo.relative_to(ruta).as_posix() for archivo in ruta.rglob('*.parquet')
        )

    def tamano(self, fuente: str) -> float:
        ruta = self._ruta(fuente)
        if not ruta.exists():
            return 0.0
        total = sum(archivo.stat().st_size for archivo in ruta.rglob('*.parquet'))
        return total / 1024 / 1024


# ------------------------------------------------------------------------ s3

@dataclass
class AjustesS3:
    endpoint: str
    bucket: str
    acceso: str
    secreto: str
    region: str = 'us-east-1'
    usar_ssl: bool = False

    @classmethod
    def desde_entorno(cls) -> AjustesS3:
        return cls(
            endpoint=os.environ.get('PIPELINE_S3_ENDPOINT', 'localhost:9000'),
            bucket=os.environ.get('PIPELINE_S3_BUCKET', 'pipeline-crudo'),
            acceso=os.environ.get('PIPELINE_S3_ACCESO', 'pruebas'),
            secreto=os.environ.get('PIPELINE_S3_SECRETO', 'pruebas'),
            region=os.environ.get('PIPELINE_S3_REGION', 'us-east-1'),
            usar_ssl=os.environ.get('PIPELINE_S3_SSL', 'false').strip().lower() == 'true',
        )

    @property
    def url(self) -> str:
        return f"{'https' if self.usar_ssl else 'http'}://{self.endpoint}"


class AlmacenS3:
    """Crudo en un bucket S3 compatible, con el mismo diseño de particiones.

    Lee y escribe con DuckDB y su extensión `httpfs`, que es la forma en que un
    motor analítico consume Parquet en la nube. boto3 se usa solo para lo que
    DuckDB no hace: crear el bucket, vaciar el prefijo y listar objetos.
    """

    nombre = 's3'

    def __init__(self, ajustes: AjustesS3 | None = None) -> None:
        self.ajustes = ajustes or AjustesS3.desde_entorno()

    # -- utilidades

    def _cliente(self):
        import boto3
        from botocore.config import Config

        return boto3.client(
            's3',
            endpoint_url=self.ajustes.url,
            aws_access_key_id=self.ajustes.acceso,
            aws_secret_access_key=self.ajustes.secreto,
            region_name=self.ajustes.region,
            config=Config(s3={'addressing_style': 'path'}, retries={'max_attempts': 5}),
        )

    def _listar(self, prefijo: str) -> list[dict]:
        """Objetos que hay bajo un prefijo.

        Un bucket que todavía no existe equivale a un prefijo vacío: es el caso
        de la primera ejecución en un entorno nuevo, y sin esto el pipeline
        fallaría antes de llegar a crear el bucket.
        """
        from botocore.exceptions import ClientError

        cliente = self._cliente()
        encontrados: list[dict] = []
        try:
            paginador = cliente.get_paginator('list_objects_v2')
            for pagina in paginador.paginate(Bucket=self.ajustes.bucket, Prefix=prefijo):
                encontrados.extend(pagina.get('Contents', []))
        except ClientError as error:
            codigo = error.response.get('Error', {}).get('Code', '')
            if codigo in {'NoSuchBucket', '404', 'NoSuchKey'}:
                return []
            raise
        return encontrados

    def _prefijo(self, fuente: str) -> str:
        return f'{PREFIJO_CRUDO}/{fuente}/'

    # -- interfaz

    def uri(self, fuente: str) -> str:
        return f"s3://{self.ajustes.bucket}/{self._prefijo(fuente)}**/*.parquet"

    def configurar(self, conexion: duckdb.DuckDBPyConnection) -> None:
        conexion.execute('install httpfs; load httpfs;')
        conexion.execute(f"set s3_endpoint='{self.ajustes.endpoint}'")
        conexion.execute(f"set s3_use_ssl={'true' if self.ajustes.usar_ssl else 'false'}")
        conexion.execute("set s3_url_style='path'")
        conexion.execute(f"set s3_access_key_id='{self.ajustes.acceso}'")
        conexion.execute(f"set s3_secret_access_key='{self.ajustes.secreto}'")
        conexion.execute(f"set s3_region='{self.ajustes.region}'")

    def asegurar_bucket(self) -> None:
        """Crea el bucket si no existe. Es idempotente."""
        cliente = self._cliente()
        try:
            cliente.head_bucket(Bucket=self.ajustes.bucket)
        except Exception:  # noqa: BLE001  (cualquier fallo significa que no está)
            cliente.create_bucket(Bucket=self.ajustes.bucket)
            log.info('bucket %s creado en %s', self.ajustes.bucket, self.ajustes.url)

    def _vaciar(self, fuente: str) -> int:
        """Borra los objetos de una fuente antes de reescribirla.

        Hace las veces del `rmtree` del almacén local: si no se vacía, un año que
        ya no venga en los datos seguiría en el bucket y el crudo mentiría.
        """
        objetos = self._listar(self._prefijo(fuente))
        if not objetos:
            return 0

        cliente = self._cliente()
        cliente.delete_objects(
            Bucket=self.ajustes.bucket,
            Delete={'Objects': [{'Key': objeto['Key']} for objeto in objetos]},
        )
        return len(objetos)

    def escribir(self, fuente: str, datos: pd.DataFrame, particion: str) -> str:
        preparado = _preparar(datos, particion)
        self.asegurar_bucket()
        anteriores = self._vaciar(fuente)
        if anteriores:
            log.info('%s: %s objetos anteriores borrados del bucket', fuente, anteriores)

        destino = f"s3://{self.ajustes.bucket}/{self._prefijo(fuente).rstrip('/')}"
        with duckdb.connect() as conexion:
            self.configurar(conexion)
            conexion.register('datos_nuevos', preparado)
            conexion.execute(
                f"copy (select * from datos_nuevos) to '{destino}' "
                f'(format parquet, partition_by ({particion}), overwrite_or_ignore true)'
            )
        return destino

    def leer(self, fuente: str) -> pd.DataFrame | None:
        if not self.inventario(fuente):
            return None
        with duckdb.connect() as conexion:
            self.configurar(conexion)
            return conexion.execute(
                f"select * from read_parquet('{self.uri(fuente)}', "
                f'hive_partitioning = true, union_by_name = true)'
            ).df()

    def inventario(self, fuente: str) -> list[str]:
        return sorted(objeto['Key'] for objeto in self._listar(self._prefijo(fuente)))

    def tamano(self, fuente: str) -> float:
        total = sum(objeto.get('Size', 0) for objeto in self._listar(self._prefijo(fuente)))
        return total / 1024 / 1024


# -------------------------------------------------------------------- factoría

def obtener(nombre: str | None = None) -> Almacen:
    """Devuelve el almacén configurado. Por defecto, disco."""
    elegido = (nombre or os.environ.get('PIPELINE_ALMACEN', 'local')).strip().lower()
    if elegido == 's3':
        return AlmacenS3()
    if elegido == 'local':
        return AlmacenLocal()
    raise ValueError(f"almacén desconocido: {elegido!r} (usa 'local' o 's3')")


def actual() -> Almacen:
    """El almacén activo según el entorno."""
    return obtener()
