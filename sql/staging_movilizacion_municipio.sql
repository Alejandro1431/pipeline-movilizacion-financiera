-- Staging: movilización mensual por municipio y departamento.
-- Grano: un registro por municipio y mes.
-- Origen: Superintendencia Financiera de Colombia (dataset xcga-ji4i).
--
-- Dos rarezas del origen, ambas verificadas contra los datos crudos:
--
--   1. Los códigos DANE vienen sin ceros a la izquierda ("5" en vez de "05"),
--      así que se normalizan a 2 y 5 dígitos.
--
--   2. Para 38 claves (de 179.500 filas, todas en 2024-2026 y repartidas en 12
--      municipios) el publicador emite DOS filas con el mismo año, mes y
--      municipio, la misma fecha de informe y valores distintos: una con el
--      movimiento normal y otra con 0 o 1 desembolso. No hay ninguna columna
--      que las distinga, así que el grano publicado no permite separarlas.
--      Decisión: agregar con SUM, que conserva el total y deja la clave única.
--      Se guarda `filas_origen` para poder auditar cuántas filas se unieron.
--
--      Contrapartida asumida: si el publicador añade más adelante una columna
--      que separe esos registros, hay que volver al grano fino. Y `clientes`
--      se suma, lo que en teoría podría contar dos veces a un mismo cliente;
--      en los 38 casos la fila extra aporta 0 o 1 cliente, así que el impacto
--      es despreciable pero queda dicho.
--
-- Nota de capas: en un proyecto con dbt esto viviría en una capa intermedia y
-- el staging sería copia fiel del origen. Aquí staging es la primera capa con
-- reglas de negocio.

create or replace table staging.movilizacion_municipio as
with tipado as (
    select
        cast(a_o as integer)                                       as anio,
        cast(mes as integer)                                       as mes,
        make_date(cast(a_o as integer), cast(mes as integer), 1)    as periodo,
        lpad(cast(try_cast(cod_dpto as bigint) as varchar), 2, '0') as codigo_departamento,
        trim(departamento)                                         as departamento,
        lpad(cast(try_cast(cod_mun as bigint) as varchar), 5, '0')  as codigo_municipio,
        trim(municipio)                                            as municipio,
        cast(ndesembolsos as bigint)                               as desembolsos,
        cast(nclientes as bigint)                                  as clientes,
        cast(total_movilizado as decimal(20, 2))                   as valor_movilizado,
        cast(fecha_de_informaci_n as date)                         as fecha_informe
    from raw.movilizacion_municipio
    where a_o is not null
      and mes is not null
      and cod_mun is not null
)
select
    anio,
    mes,
    periodo,
    codigo_departamento,
    departamento,
    codigo_municipio,
    municipio,
    sum(desembolsos)        as desembolsos,
    sum(clientes)           as clientes,
    sum(valor_movilizado)   as valor_movilizado,
    max(fecha_informe)      as fecha_informe,
    count(*)                as filas_origen
from tipado
group by all;
