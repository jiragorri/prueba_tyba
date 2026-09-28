# Take-Home Assignment — Data Engineering | Tyba

Pipeline reproducible para ingerir cortes Parquet de movimientos financieros, conservar su historial en DuckDB, comparar cortes consecutivos y generar un resumen de calidad e insights.

## Documentación

- [Arquitectura y funcionamiento](docs/ARQUITECTURA_Y_FUNCIONAMIENTO.md): sustento técnico, flujo interno, modelo de datos, reglas de comparación y limitaciones.
- [Guía de ejecución y validación](docs/GUIA_EJECUCION_Y_VALIDACION.md): requisitos, comandos, pruebas, Docker, verificación de resultados y solución de problemas.

La documentación está pensada para explicar tanto la solución implementada como la forma de reproducir y validar una ejecución completa.

## Ejecutar con Docker

Con Docker y Docker Compose instalados, desde la raíz del repositorio:

```sh
docker compose up --build
```

No requiere variables de entorno ni configuración adicional. Los Parquet se montan como solo lectura desde `data/raw/`; la base de datos y los reportes se escriben en `data/output/`.

## Ejecutar localmente

Se requiere Python 3.9 o superior.

```sh
python -m pip install -r requirements.txt
python -m src.pipeline
python -m unittest discover -s test -v
```

Se pueden cambiar las carpetas de entrada y salida con `--input-dir` y `--output-dir`.

## Arquitectura

- **DuckDB** lee los Parquet de forma vectorizada y persiste las observaciones en `data/output/tyba.duckdb`.
- Cada archivo es un corte inmutable identificado por nombre y SHA-256 del contenido. Volver a ejecutar el pipeline no duplica un corte ya cargado; un nuevo archivo se incorpora en una transacción.
- Los nombres se ordenan naturalmente (`T`, `T1`, `T2`, …). Los nombres deben preservar el orden cronológico y los cortes no deben sobrescribirse.
- `snapshots` registra los cortes; `snapshot_rows` conserva todas las filas de cada uno; `change_events` clasifica los cambios entre cortes consecutivos.
- La vista `current_movements` expone las filas del último corte cargado.
- `data/output/insights.md` y `data/output/insights.json` se regeneran en cada ejecución. El archivo DuckDB y los reportes generados no se versionan.

### Identidad de los movimientos

El PDF describe `id` como identificador de transacción, pero los Parquet entregados no tienen una columna `id`: contienen `id_cliente`, que no es único (el primer corte tiene 50.000 filas y 3.000 identificadores distintos). Por ello no es posible demostrar una identidad estable de transacción.

Como alternativa explícita, el pipeline usa una **clave compuesta candidata** formada por `id_cliente`, fecha normalizada, `product`, `fund` y `commercial_name`. `type`, `amount` y `description` quedan fuera de esa clave y sus cambios se pueden detectar cuando la clave aparece una sola vez en ambos cortes. Las fechas `YYYY-MM-DD` y `DD/MM/YYYY` se convierten a una fecha canónica.

- Clave única en ambos cortes y contenido igual: `UNCHANGED`.
- Clave única en ambos cortes y otro campo distinto: `CORRECTED`.
- Clave presente solo en el corte nuevo/anterior: `NEW`/`REMOVED`.
- Clave repetida en cualquiera de los cortes: `AMBIGUOUS`; no se inventa una correspondencia.

Cambios en campos incluidos en la clave compuesta se verán como una baja y un alta. Se necesita un identificador de transacción estable del origen para resolver esto con certeza y detectar todas las correcciones. El pipeline rechaza columnas ausentes o inesperadas para evitar descartar silenciosamente cambios de esquema.

## Calidad e insights

Las decisiones de calidad son conservadoras: no se eliminan ni se corrigen filas; se conservan valores nulos, montos negativos y ceros. Se aceptan las dos representaciones de fecha observadas y se informa cualquier fecha que no se pueda parsear. Para analítica, `ENTRADA`/`Entrada`/`entrada` y `IN` se agrupan como `IN`; `SALIDA`/`Salida`/`salida` y `OUT` como `OUT`. Los valores fuente permanecen sin modificar.

En los archivos suministrados, el reporte actual identifica 49.000 filas y 3.000 identificadores de cliente en `movimientos_dia_T1.parquet`. También reporta 1.440 montos nulos, 4.485 descripciones nulas y 8.167 nombres comerciales nulos; las 49.000 fechas se parsean, aunque el origen mezcla los dos formatos indicados. Para la comparación T → T1, la clave candidata clasifica 35.129 sin cambio, 3.840 corregidas, 9.991 nuevas, 10.991 removidas y 24 claves ambiguas. Estos resultados son específicos de los Parquet actuales; el reporte se recalcula en cada ejecución.

Las sumas de montos excluyen los valores nulos y usan los valores numéricos de origen, sin inferir una moneda ni recalcular el signo según el tipo de movimiento. Los montos negativos y cero se cuentan por separado en el reporte.

## Consultas útiles

Las tablas se pueden consultar con DuckDB, por ejemplo:

```sql
SELECT event_type, count(*)
FROM change_events
GROUP BY event_type
ORDER BY event_type;

SELECT snapshot_name, count(*) AS movement_count
FROM current_movements
GROUP BY snapshot_name;
```

Para agregar otro corte diario, colóquelo en `data/raw/` con un nombre que ordene después de los anteriores (por ejemplo, `movimientos_dia_T2.parquet`) y vuelva a ejecutar. Mantenga los cortes previos para conservar la secuencia completa de comparaciones.