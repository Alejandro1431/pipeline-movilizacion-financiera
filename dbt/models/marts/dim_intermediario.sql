-- Dimensión de intermediarios financieros.
--
-- El origen anonimiza los nombres, pero deja el tipo de entidad. Y una entidad
-- puede cambiar de tipo con el tiempo: DECEVAL aparece como ADMINISTRADOR DE
-- BONOS entre marzo de 2021 y abril de 2025, y como FINTECH desde diciembre de
-- 2025. Eso lo detectó la prueba que exigía un solo tipo por código, que era un
-- supuesto mío y resultó falso.
--
-- Decisión: el grano de la dimensión es (nombre, tipo), no el nombre. Así el
-- cambio queda representado en vez de escondido, cada tipo conserva el periodo
-- en el que estuvo vigente, y los hechos se atribuyen al tipo que correspondía
-- en su mes. Es un cambio de atributo a lo largo del tiempo (SCD tipo 2)
-- resuelto con las fechas de vigencia observadas en los propios datos.
--
-- La clave es un hash de (nombre, tipo) para poder declarar la relación desde
-- los hechos con una sola columna.

select
    md5(nombre_intermediario || '|' || tipo_intermediario) as id_intermediario,
    nombre_intermediario,
    tipo_intermediario,
    min(periodo)    as vigente_desde,
    max(periodo)    as vigente_hasta,
    count(*)        as meses_reportados
from {{ source('staging', 'movilizacion_intermediario') }}
group by 1, 2, 3
