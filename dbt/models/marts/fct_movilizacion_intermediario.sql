-- Tabla de hechos: movilización por intermediario financiero y mes.
--
-- Grano: un registro por (periodo, nombre_intermediario).
-- `id_intermediario` apunta a la versión de la dimensión que corresponde al tipo
-- que la entidad tenía ese mes, no al tipo actual.

select
    periodo,
    md5(nombre_intermediario || '|' || tipo_intermediario) as id_intermediario,
    nombre_intermediario,
    cast(desembolsos as bigint)                 as desembolsos,
    cast(clientes as bigint)                    as clientes,
    cast(total_desembolso as decimal(20, 2))    as total_desembolso
from {{ source('staging', 'movilizacion_intermediario') }}
