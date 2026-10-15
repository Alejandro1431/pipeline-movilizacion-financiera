# Caso de estudio: pipeline de movilización financiera

Este documento cuenta el proyecto como lo contaría en una entrevista: qué problema
había, qué construí, qué decidí y qué me costó. El código está en el resto del
repositorio; aquí está el criterio.

## El problema

La Superintendencia Financiera y Findeter publican datos de crédito y movilización
financiera en datos.gov.co, pero publicarlos no es lo mismo que poder usarlos:

- Cada respuesta de la API llega con **todas las columnas como texto**, incluso los
  importes y los códigos.
- Un mismo municipio aparece con el código `5` en unas filas y `05` en otras.
- Un dataset trae los valores como `'$    7,000,000,000.00'` y la ubicación como un
  objeto GeoJSON anidado.
- Nada garantiza que la carga de mañana tenga la misma forma que la de hoy.

Responder una pregunta tan simple como *cuánto se movilizó por departamento el año
pasado* obligaba a descargar ficheros, limpiarlos a mano y esperar que nadie
hubiera roto el formato. Ese es el trabajo que este pipeline automatiza.

## Qué construí

Un pipeline ELT que va de la API pública a un warehouse consultable:

1. **Extracción** paginada por la API Socrata, con reintentos y orden estable, que
   escribe Parquet particionado por año, en disco o en un bucket S3 compatible. Con
   dos modos: completo e incremental por marcas de agua.
2. **Staging** en DuckDB: tipado, normalización de códigos y limpieza de la fuente
   sucia.
3. **Modelo dimensional** en dbt: tres dimensiones, dos tablas de hechos y un
   agregado para consumo, con las relaciones declaradas y probadas.
4. **Calidad**: 15 reglas sobre el staging y el crudo, y 29 pruebas sobre el modelo
   dimensional, todas capaces de detener el proceso.
5. **Orquestación** con Prefect: cinco tareas con dependencias explícitas,
   reintentos solo donde sirven de algo y un historial de ejecuciones.
6. **Perfilado**: un comando que responde las preguntas de negocio directamente
   desde el warehouse.

| | |
|---|---|
| Fuentes | 3 datasets públicos |
| Filas cargadas | 194.832 |
| Cobertura | enero 2009 a septiembre 2026 |
| Modelos de dbt | 6 |
| Reglas de calidad | 15 |
| Pruebas | 88 de Python y 29 de dbt |
| Ahorro de la carga incremental | 93% menos transferencia |
| Tiempo de ejecución | unos 40 segundos completo, 21 incremental |

## Decisiones técnicas

**Refresco completo en lugar de incremental.** El origen no tiene un campo de
actualización fiable por fila y el volumen son 3 MB. Se implementó igual un filtro
incremental opcional (`--desde`) para cuando el volumen lo justifique, pero no se
activó por defecto: la complejidad no estaba pagada.

**Paginación con `$order=:id`.** Sin un orden explícito, Socrata no garantiza que
dos páginas consecutivas no repitan o se salten filas. Es el error clásico de este
tipo de extracción y hay una prueba unitaria dedicada a él.

**SQL para lo tipado, Python para el parseo.** Las dos fuentes limpias se resuelven
con SQL, que expresa mejor un `cast` y una normalización de códigos. La fuente sucia
necesita lógica con pruebas propias. Se podría hacer todo con funciones de Python
dentro de DuckDB, pero se perdería poder probarlo sin levantar la base.

**El almacén del crudo es una capa, no un detalle.** Escribir y leer Parquet vive
detrás de una interfaz con dos implementaciones: disco y bucket S3. El modo por
defecto es disco a propósito, porque clonar el repositorio y ejecutarlo no debe
exigir levantar ningún servicio, y la nube se activa con una variable de entorno.
El trabajo con S3 lo hace DuckDB con `httpfs`: la escritura es un
`COPY ... TO 's3://…'` con particiones y la lectura es el mismo `read_parquet` que
en disco, así que solo cambia la URI. `boto3` se usa para lo que DuckDB no hace,
que es crear el bucket, vaciar el prefijo antes de reescribirlo y listar objetos.

**Las reglas de calidad son consultas.** Cada regla es un `select` que devuelve
cuántas filas la violan. Si alguna falla, el proceso termina con código 1, y eso es
lo que hace que la integración continua se ponga en rojo sola, sin infraestructura
adicional.

## Los cinco retos que me costaron

### 1. La regla de unicidad falló, y el problema estaba en el origen

La primera ejecución terminó con 38 violaciones de la clave (año, mes, municipio).
Descarté las tres hipótesis fáciles antes de entenderlo:

- *¿Las introdujo mi pipeline?* No: el crudo tenía las mismas 179.500 filas.
- *¿Son revisiones del dato en fechas distintas?* No: las 38 claves duplicadas
  comparten la misma fecha de informe.
- *¿Cambió el esquema del origen y me faltó una columna?* No: las 10 columnas son
  idénticas en 2009 y en 2026.

Son **filas distintas que el publicador emite sin ninguna columna que las
distinga**. Sabaneta, septiembre de 2026: una fila con 120 desembolsos y 6.446
millones, y otra con 1 desembolso y 440.386 pesos.

Resolverlo borrando el duplicado habría descartado información y escondido el
problema. Decidí **agregar con `SUM`**: conserva el total, deja la clave única y no
pierde nada. Añadí una columna `filas_origen` para que cualquiera pueda auditar
cuántas filas se unieron, y una regla que vigila que sigan siendo marginales (hoy
38 de 179.462, un 0,02%). La contrapartida queda escrita en el SQL: si el
publicador añade la columna que falta, hay que volver al grano fino, y `clientes`
se suma, lo que en teoría podría contar dos veces a un mismo cliente.

### 2. La columna de partición no existía al leer

Escribí el crudo con `partition_cols=['a_o']` y las lecturas fallaban. Al
inspeccionar el esquema del archivo, la columna no estaba: pyarrow **no guarda la
columna de partición dentro del Parquet**, la codifica en la ruta (`a_o=2024/`).
Hay que leer con `hive_partitioning = true` o el año simplemente no existe. Lo dejé
comentado en el código porque es un error que se comete una vez y se olvida.

### 3. ¿Estaba limpiando bien el dinero?

El parser de moneda tenía pruebas unitarias, pero las pruebas las había escrito yo
con formatos que yo imaginé. Antes de dar por buena la limpieza, comprobé las 2.982
filas reales:

- Existen **6 familias de formato**, todas del mismo patrón (`$` + espacios + coma
  de miles + punto decimal). Ninguna trae paréntesis, negativos ni letras.
- **0 filas** con caracteres inesperados y **0** sin el símbolo `$`.
- Cotejo cruzado: sumar con el parser de Python, sumar con una expresión regular en
  SQL y sumar la tabla de staging dan **el mismo total exacto**
  (15.503.758.022.233,00).

De paso descubrí que un comentario mío afirmaba que también aparecían formatos
europeos (`1.234,56`). Era falso: lo había supuesto. La rama sigue en el código
aunque nunca haya visto ese formato, y ahora lo dice.

### 4. La prueba que demostró que mi supuesto era falso

Al construir la dimensión de intermediarios asumí que cada código de entidad tenía
un solo tipo, y escribí una prueba que lo exigía. **Falló.**

No lo arreglé borrando la prueba. Investigué: de 278 códigos, **uno** aparecía con
dos tipos, DECEVAL, el depósito de valores colombiano, que el publicador
reclasificó de *ADMINISTRADOR DE BONOS* (marzo 2021 a abril 2025) a *FINTECH*
(diciembre 2025). Comprobé además que nunca aparece con dos tipos en el mismo mes:
no era suciedad, era un cambio real en el tiempo.

Mi primera versión de la dimensión se quedaba con un tipo mediante `min()`. Eso
habría sido peor que un error visible: habría atribuido los nueve meses históricos
al tipo nuevo, en silencio. Cambié el grano de la dimensión a (nombre, tipo), con
las fechas de vigencia tomadas de los propios datos, y los hechos ahora apuntan a
la versión que correspondía en su mes. La prueba que falló se sustituyó por el
invariante que sí se cumple, que una entidad no puede tener dos versiones en el
mismo mes, y el hallazgo quedó escrito en el modelo.

### 5. La carga incremental, y el comodín que Socrata no acepta

Para no volver a descargar 194.832 filas cada vez quería pedir solo lo publicado
desde la última carga, y para fusionarlo sin duplicar necesitaba el identificador
de fila que expone Socrata (`:id`). El primer intento fue `$select=:id,*`, que es
lo natural: el identificador más todo lo demás. **La API devolvió un 400.**

Probar combinaciones aisló la causa: con `$select=:id,*` falla; con
`$select=:id,a_o,mes` funciona. El comodín no se puede mezclar con el
identificador. Así que las columnas hay que enumerarlas, y escribirlas a mano
habría sido frágil: si el publicador añade una columna, el pipeline la perdería en
silencio.

La solución fue leer el esquema del propio origen (la API de metadatos publica los
nombres de campo) y construir la consulta en cada ejecución. Efecto secundario que
no buscaba: ahora el pipeline compara las columnas descargadas con las del crudo
anterior y avisa en el log si alguna aparece, cambia de nombre o desaparece.

Con eso, la carga incremental pasó a traer 13.978 filas en vez de 194.832 y a bajar
de 40 a 21 segundos. Y lo importante no es la velocidad: es que se comprobó que
**no cambia nada**, con los mismos conteos en el crudo y en el staging, la misma
suma de valor movilizado y cero identificadores repetidos.

## Qué mejoraría

- **Migrar el staging a dbt.** La capa de marts ya vive ahí, pero el tipado y la
  normalización del staging siguen siendo dos archivos SQL que ejecuta Python.
  Tener todo el SQL en una sola herramienta unificaría el linaje y las pruebas.
- **Reglas de calidad unificadas.** Hoy conviven mis 15 reglas escritas a mano
  sobre el staging y el crudo, y las 29 pruebas de dbt sobre los marts. Funcionan y
  son legibles, pero en un proyecto real usaría un solo mecanismo, y probablemente
  Great Expectations o dbt tests para todo.
- **Orquestación con dependencias.** Cinco tareas con dependencias explícitas y
  reintentos donde tiene sentido. La decisión de no paralelizar está razonada en el
  propio código: DuckDB admite un solo escritor por archivo.
- **Desplegar el orquestador.** Corre en local y en CI, pero sin una instancia
  permanente que avise cuando una ejecución falla.
- **Probar contra un S3 real.** La integración se verifica contra un servidor S3
  simulado en el CI; un bucket de AWS o de Cloudflare R2 podría diferir en detalles
  de red y permisos.

## Por qué me sirvió este proyecto

Sobre todo por la parte que no se ve en el código. Aprendí a llevar un dato desde
una API pública hasta una tabla en la que se puede confiar, y aprendí que cuando una
validación falla lo que toca es investigar en lugar de desactivarla. La historia de
las 38 filas resume el proyecto entero: el valor no está en el código que escribe
datos, está en entender por qué no cuadran.
