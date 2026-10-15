-- Dimensión geográfica: un registro por municipio, con su departamento.
--
-- El código DANE es la clave natural. El staging ya lo normaliza a cinco dígitos
-- con ceros a la izquierda, que es lo que permite cruzarlo con otras fuentes.

select
    codigo_municipio,
    municipio,
    codigo_departamento,
    departamento
from {{ source('staging', 'movilizacion_municipio') }}
group by all
