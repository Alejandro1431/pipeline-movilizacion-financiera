-- Prueba singular: una entidad no puede apuntar a dos versiones de la dimensión
-- en el mismo mes.
--
-- Es el invariante que sí se cumple en el origen (0 casos en 12.350 filas) y el
-- que permite atribuir cada hecho a una sola versión. Si algún mes apareciera con
-- dos tipos, la atribución sería ambigua y habría que decidirlo con el publicador.

select
    periodo,
    nombre_intermediario,
    count(distinct id_intermediario) as versiones
from {{ ref('fct_movilizacion_intermediario') }}
group by 1, 2
having count(distinct id_intermediario) > 1
