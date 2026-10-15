-- Prueba singular: ninguna medida de los hechos puede ser negativa.

select *
from {{ ref('fct_movilizacion_municipio') }}
where desembolsos < 0
   or clientes < 0
   or valor_movilizado < 0
