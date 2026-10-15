-- Prueba singular: el mart agregado no puede perder ni duplicar dinero.
--
-- Compara el total del mart por departamento contra el total de los hechos.
-- Es la prueba que demuestra que la agregación es correcta, y la que detectaría
-- un join mal hecho que multiplicara filas.

with hechos as (

    select sum(valor_movilizado) as total from {{ ref('fct_movilizacion_municipio') }}

),

mart as (

    select sum(valor_movilizado) as total from {{ ref('mart_movilizacion_departamento_mes') }}

)

select
    hechos.total  as total_hechos,
    mart.total    as total_mart,
    hechos.total - mart.total as diferencia
from hechos, mart
where abs(hechos.total - mart.total) > 0.01
