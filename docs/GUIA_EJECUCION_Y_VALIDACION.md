# Guía de ejecución y validación

## 1. Requisitos

### Ejecución local

- Python 3.9 o superior.
- Acceso a los archivos Parquet en `data/raw/`.
- Dependencias instaladas desde `requirements.txt`.

### Ejecución con Docker

- Docker Desktop iniciado.
- Docker Compose disponible mediante `docker compose`.

## 2. Preparar el entorno local

Desde la raíz del repositorio:

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

En Windows, la activación equivalente es:

```powershell
.venv\Scripts\Activate.ps1
```

## 3. Ejecutar las pruebas

El proyecto usa la biblioteca estándar `unittest`, por lo que no requiere instalar `pytest`:

```sh
python -m unittest discover -s test -v
```

Las pruebas cubren:

1. comparación de dos snapshots y clasificación de cambios;
2. idempotencia al ejecutar dos veces el mismo conjunto de archivos;
3. tratamiento conservador de claves ambiguas;
4. rechazo de un esquema Parquet incompatible.

Resultado esperado:

```text
Ran 3 tests
OK
```

## 4. Ejecutar el pipeline local

```sh
python -m src.pipeline
```

El comando usa estos valores por defecto:

- entrada: `data/raw`
- salida: `data/output`

También se pueden indicar carpetas distintas:

```sh
python -m src.pipeline \
  --input-dir ruta/a/parquet \
  --output-dir ruta/de/salida
```

Durante la ejecución se muestran mensajes como:

```text
INFO: Loaded movimientos_dia_T.parquet (50000 rows)
INFO: Loaded movimientos_dia_T1.parquet (49000 rows)
INFO: Database and insights written under .../data/output
```

Si un archivo ya fue cargado con el mismo nombre y hash, se muestra `Skipping already loaded snapshot`.

## 5. Ejecutar con Docker Compose

Desde la raíz del proyecto:

```sh
docker compose up --build
```

El servicio monta:

- `./data/raw` en `/app/data/raw` como solo lectura;
- `./data/output` en `/app/data/output` con escritura.

El contenedor ejecuta `python -m src.pipeline` y finaliza cuando termina el procesamiento. Un resultado correcto termina con:

```text
pipeline-1 exited with code 0
```

Para revisar el estado después de la ejecución:

```sh
docker compose ps -a
```

Para consultar los logs de una ejecución ya realizada:

```sh
docker compose logs pipeline
```

## 6. Verificar los resultados

Después de una ejecución deben existir:

```text
data/output/
├── insights.json
├── insights.md
└── tyba.duckdb
```

Para inspeccionar el reporte Markdown:

```sh
less data/output/insights.md
```

Para consultar el JSON:

```sh
python -m json.tool data/output/insights.json
```

Para consultar DuckDB, se puede usar el cliente de DuckDB si está instalado:

```sh
duckdb data/output/tyba.duckdb
```

Consultas útiles:

```sql
SELECT source_name, source_row_count, snapshot_order
FROM snapshots
ORDER BY snapshot_order;

SELECT event_type, count(*) AS total
FROM change_events
GROUP BY event_type
ORDER BY event_type;

SELECT snapshot_name, count(*) AS movement_count
FROM current_movements
GROUP BY snapshot_name;
```

## 7. Validación de una ejecución completa

La validación recomendada combina tres controles:

### Control funcional

```sh
python -m unittest discover -s test -v
```

Confirma que las reglas de negocio principales siguen funcionando.

### Control de ejecución

```sh
python -m src.pipeline
```

Confirma que los Parquet reales pueden leerse, validarse y persistirse.

### Control reproducible

```sh
docker compose up --build
```

Confirma que el mismo flujo funciona dentro de la imagen declarada por el proyecto.

Una corrida válida debe cumplir:

- pruebas con resultado `OK`;
- pipeline local sin excepciones;
- contenedor con código de salida `0`;
- generación o actualización de los tres artefactos de salida;
- ausencia de duplicación al repetir la ejecución con los mismos Parquet.

## 8. Incorporar un nuevo corte

1. Copiar el nuevo archivo Parquet en `data/raw/`.
2. Usar un nombre que preserve el orden, por ejemplo `movimientos_dia_T2.parquet`.
3. No sobrescribir los archivos anteriores.
4. Ejecutar nuevamente el pipeline.
5. Revisar `insights.md` y los eventos de `change_events`.

Ejemplo:

```sh
cp nuevo_corte.parquet data/raw/movimientos_dia_T2.parquet
python -m src.pipeline
```

El pipeline conserva los cortes anteriores y compara el nuevo con el snapshot inmediatamente anterior según el orden natural del nombre.

## 9. Problemas frecuentes

### No se encuentran archivos

```text
No .parquet snapshots found in ...
```

Verificar que existan archivos con extensión `.parquet` en la carpeta indicada por `--input-dir`.

### Esquema incompatible

```text
Unsupported Parquet schema ...
```

Comparar las columnas del archivo con las ocho columnas requeridas. También se rechazan columnas adicionales.

### Docker no inicia

Comprobar que Docker Desktop esté abierto y ejecutar:

```sh
docker version
docker compose version
```

### El contenedor termina rápidamente

Esto es esperado: el servicio ejecuta un proceso batch, no un servidor permanente. Revisar los mensajes con `docker compose logs pipeline`.

### Los cambios aparecen como `AMBIGUOUS`

Esto ocurre cuando la clave compuesta candidata se repite. No es un error del motor: indica que el origen no permite emparejar las filas de forma segura.
