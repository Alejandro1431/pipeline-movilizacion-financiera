-- Dimensión de tiempo: un registro por mes con los atributos de calendario.
--
-- Se construye a partir de los periodos que aparecen en los hechos, no de una
-- tabla de calendario generada: así la dimensión solo contiene meses con datos.

with periodos as (

    select distinct periodo from {{ source('staging', 'movilizacion_municipio') }}
    union
    select distinct periodo from {{ source('staging', 'movilizacion_intermediario') }}

)

select
    periodo,
    cast(extract(year from periodo) as integer)                         as anio,
    cast(extract(month from periodo) as integer)                        as mes,
    cast(extract(quarter from periodo) as integer)                      as trimestre,
    case when extract(month from periodo) <= 6 then 1 else 2 end        as semestre,
    strftime(periodo, '%Y-%m')                                          as etiqueta_mes,
    strftime(periodo, '%Y') || '-T' || cast(extract(quarter from periodo) as integer)
                                                                        as etiqueta_trimestre
from periodos
where periodo is not null
