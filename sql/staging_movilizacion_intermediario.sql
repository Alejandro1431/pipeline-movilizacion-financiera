-- Staging: movilización mensual por intermediario financiero.
-- Grano: un registro por intermediario y mes.
-- Origen: Superintendencia Financiera de Colombia (dataset xbk8-edux).
--
-- Advertencia del origen: los nombres vienen anonimizados por el publicador
-- (por ejemplo "BAN100 SA"), así que este dataset no se puede cruzar contra el
-- nombre real de la entidad. Se conserva el código tal cual llega.

create or replace table staging.movilizacion_intermediario as
select
    cast(a_o as integer)                                     as anio,
    cast(mes as integer)                                     as mes,
    make_date(cast(a_o as integer), cast(mes as integer), 1) as periodo,
    trim(tipo_intermediario)                                 as tipo_intermediario,
    trim(nombre_intermediario)                               as nombre_intermediario,
    cast(ndesembolsos as bigint)                             as desembolsos,
    cast(nclientes as bigint)                                as clientes,
    cast(total_desembolso as decimal(20, 2))                 as total_desembolso,
    cast(fecha_de_informaci_n as date)                       as fecha_informe
from raw.movilizacion_intermediario
where a_o is not null
  and mes is not null
  and nombre_intermediario is not null;
