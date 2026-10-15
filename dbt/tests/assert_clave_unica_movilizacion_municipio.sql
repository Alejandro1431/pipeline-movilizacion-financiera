-- Prueba singular: la clave (periodo, codigo_municipio) es única en los hechos.
-- Si devuelve filas, la prueba falla.

select
    periodo,
    codigo_municipio,
    count(*) as filas
from {{ ref('fct_movilizacion_municipio') }}
group by 1, 2
having count(*) > 1
