# Insights del pipeline

## Corte más reciente

- Archivo: `movimientos_dia_T1.parquet`
- Filas: 49,000
- Identificadores de cliente distintos: 3,000
- Suma de montos no nulos: 1,113,305,841,982.35
- Montos negativos: 989; montos en cero: 903

## Suma de montos por tipo de movimiento normalizado

Los montos se suman tal como vienen en los archivos; los valores nulos se excluyen de la suma.

| Tipo | Filas | Monto total | Montos nulos |
| --- | ---: | ---: | ---: |
| IN | 27,100 | 595,058,249,885.29 | 772 |
| OUT | 21,900 | 518,247,592,097.06 | 668 |

## Calidad de los datos

| Validación | Filas |
| --- | ---: |
| `id_cliente` nulo | 0 |
| `date` nulo | 0 |
| `product` nulo | 0 |
| `type` nulo | 0 |
| `fund` nulo | 0 |
| `amount` nulo | 1,440 |
| `description` nulo | 4,485 |
| `commercial_name` nulo | 8,167 |
| Fechas no interpretables | 0 |
| Fechas con formato `YYYY-MM-DD` | 45,567 |
| Fechas con formato `DD/MM/YYYY` | 3,433 |

Los dos formatos de fecha observados se convierten a una fecha normalizada. Se conservan los montos nulos y con signo; no se corrigen silenciosamente.

## Cambios entre cortes

`CORRECTED` indica que la clave candidata coincide de forma única, pero cambió otro campo. Las claves `AMBIGUOUS` no se emparejan automáticamente.

| Desde | Hasta | Evento | Claves candidatas |
| --- | --- | --- | ---: |
| movimientos_dia_T.parquet | movimientos_dia_T1.parquet | Ambiguo (AMBIGUOUS) | 24 |
| movimientos_dia_T.parquet | movimientos_dia_T1.parquet | Corregido (CORRECTED) | 3,840 |
| movimientos_dia_T.parquet | movimientos_dia_T1.parquet | Nuevo (NEW) | 9,991 |
| movimientos_dia_T.parquet | movimientos_dia_T1.parquet | Eliminado (REMOVED) | 10,991 |
| movimientos_dia_T.parquet | movimientos_dia_T1.parquet | Sin cambios (UNCHANGED) | 35,129 |

## Análisis interpretativo

El corte más reciente muestra un volumen de 49.000 movimientos y una base estable en torno a 3.000 clientes distintos, con un balance positivo de 1,113,305,841,982.35 en monto acumulado no nulo. La distribución por tipo indica una ligera ventaja de entradas sobre salidas: 27.100 movimientos de tipo IN frente a 21.900 de tipo OUT, lo que sugiere un flujo neto de efectivo positivo en el período observado.

En términos de calidad, el conjunto es razonablemente consistente: no hay fechas nulas ni no parseables y la mayor parte de los valores usan formato ISO (45.567 filas, aproximadamente 93%). Sin embargo, hay 1.440 montos nulos, 4.485 descripciones vacías y 8.167 nombres comerciales nulos. Esos vacíos no invalidan el corte, pero sí reducen la capacidad analítica si se quiere segmentar por producto o canal comercial sin una limpieza previa.

La comparación T → T1 sugiere una operación con movimiento moderado pero no trivial. Aproximadamente 35.129 claves permanecen sin cambios (71,7%), lo que indica que la mayor parte de la base es estable. Aun así, se observan 24.822 cambios no triviales entre ambos cortes: 3.840 corregidas, 9.991 nuevas y 10.991 eliminadas. Esto es consistente con un escenario donde la cartera o los movimientos reales cambian de forma continua, pero no de forma caótica ni totalmente desordenada. La presencia de 24 claves ambiguas confirma que no todas las coincidencias pueden emparejarse con seguridad sin un identificador de transacción estable; en esos casos el pipeline adopta una política conservadora y evita inventar correspondencias.

En conjunto, el dato es útil para monitorizar evolución de movimientos, detectar cambios netos y priorizar limpieza de registros incompletos; sin embargo, para un análisis financiero más preciso y para automatizar decisiones operativas, seguiría siendo necesario contar con un identificador único de transacción en la fuente.

## Limitación de identidad

Los archivos proporcionados contienen id_cliente, no el identificador de transacción descrito en el PDF. Por ello, movement_key es una clave compuesta candidata basada en cliente, fecha normalizada, producto, fondo y nombre comercial. Las claves duplicadas se clasifican como AMBIGUOUS en vez de adivinar una correspondencia. Sin un identificador de transacción estable no se pueden emparejar de forma fiable los cambios en los campos que forman la clave.
