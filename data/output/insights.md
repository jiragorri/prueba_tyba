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

## Limitación de identidad

Los archivos proporcionados contienen id_cliente, no el identificador de transacción descrito en el PDF. Por ello, movement_key es una clave compuesta candidata basada en cliente, fecha normalizada, producto, fondo y nombre comercial. Las claves duplicadas se clasifican como AMBIGUOUS en vez de adivinar una correspondencia. Sin un identificador de transacción estable no se pueden emparejar de forma fiable los cambios en los campos que forman la clave.
