-- Mart de consumo: movilización agregada por departamento y mes.
--
-- Es la tabla que consumiría un tablero, ya con las dimensiones resueltas.
--
-- Nota sobre las medidas: se agregan `desembolsos` y `valor_movilizado` porque
-- son totalmente aditivas. `clientes` NO se agrega a propósito: sumar los
-- clientes de todos los municipios de un departamento cuenta varias veces a
-- quien opera en más de un municipio, y un número no aditivo presentado como
-- total es una métrica engañosa.

select
    t.periodo,
    t.anio,
    t.mes,
    t.etiqueta_mes,
    g.codigo_departamento,
    g.departamento,
    sum(f.desembolsos)          as desembolsos,
    sum(f.valor_movilizado)     as valor_movilizado,
    count(distinct f.codigo_municipio) as municipios_con_movilizacion
from {{ ref('fct_movilizacion_municipio') }} f
inner join {{ ref('dim_geografia') }} g on g.codigo_municipio = f.codigo_municipio
inner join {{ ref('dim_tiempo') }} t on t.periodo = f.periodo
group by all
