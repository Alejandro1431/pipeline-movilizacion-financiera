-- Tabla de hechos: movilización por municipio y mes.
--
-- Grano: un registro por (periodo, codigo_municipio). Las claves son naturales
-- (el mes y el código DANE) en lugar de subrogadas, porque ya son estables y
-- legibles; las pruebas de relaciones garantizan que existen en las dimensiones.

select
    periodo,
    codigo_municipio,
    cast(desembolsos as bigint)                 as desembolsos,
    cast(clientes as bigint)                    as clientes,
    cast(valor_movilizado as decimal(20, 2))    as valor_movilizado,
    cast(filas_origen as integer)               as filas_origen
from {{ source('staging', 'movilizacion_municipio') }}
