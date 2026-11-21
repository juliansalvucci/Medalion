# SPEC: Pipeline Ecobici (Bronze / Silver / Gold)

Spec para implementar con opencode. Fuente: enunciado "Ejercicio: pipeline de datos con arquitectura Bronze, Silver y Gold" (UTN FRVM, Ciencia de Datos).

## 0. Reglas de trabajo para el agente

- Idioma de la documentación (README.md, DECISIONES.md) y de los comentarios: **español**. Nombres de columnas, tablas y funciones: exactamente los del enunciado.
- Herramienta: **Python 3.11+ con pandas** (+ `pyarrow` para Parquet). No usar Spark, Polars ni DuckDB en el pipeline.
- Todo el código en **funciones**. Ningún script depende de que otro se haya corrido a mano. `python main.py` corre todo de principio a fin.
- **Idempotencia**: correr el pipeline dos veces produce el mismo resultado. Cada salida se escribe en una ruta fija y **reemplaza** la anterior. Prohibido hacer append a archivos existentes.
- Los datos **nunca se modifican en el lugar**: cada capa lee la anterior y escribe una nueva. `data/raw/` es de solo lectura.
- No inventar nombres de columnas ni formatos de los archivos. **Se descubren inspeccionando los archivos reales** (ver Fase 1). Si falta algún archivo en `data/raw/`, detenerse y pedirle al usuario que lo descargue (no editarlo ni descomprimirlo).
- Toda decisión no indicada en el enunciado se anota en `DECISIONES.md` con una línea de justificación.
- Trabajar por fases y **verificar cada fase antes de pasar a la siguiente** (criterios de aceptación al final de cada una).

## 1. Estructura del proyecto (obligatoria)

```
ecobici-pipeline/
├── data/
│   ├── raw/          # ZIP/CSV descargados, sin tocar
│   ├── bronze/
│   ├── silver/
│   └── gold/
│       └── reportes/
├── src/
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   └── reportes.py
├── main.py
├── DECISIONES.md
├── README.md
└── requirements.txt
```

Agregar `src/__init__.py` y un `.gitignore` que excluya `data/raw/`, `data/bronze/`, `data/silver/` y `data/gold/*` salvo `data/gold/reportes/` (el enunciado pide entregar solo los reportes). Se permiten módulos auxiliares en `src/` (por ejemplo `config.py` para rutas y constantes) siempre que las cuatro capas pedidas existan.

## 2. Datos de entrada

Dataset: https://data.buenosaires.gob.ar/dataset/bicicletas-publicas

Años: **2016, 2019, 2024**. Por año hay dos archivos en `data/raw/`:
- **Recorridos**: ZIP, un registro por viaje, millones de filas.
- **Usuarios**: CSV (versión CSV, no XLSX), un registro por usuario.

Los nombres de archivo exactos los define el usuario al descargarlos. El código debe **detectar los archivos por año** (patrón sobre el nombre: contiene el año, y "recorrido" o "usuario") y fallar con un mensaje claro si falta alguno. Los archivos de distintos años **no tienen el mismo formato** (separador, nombres de columnas, formato de fecha, presencia/unidad de duración, presencia de id de usuario, posiblemente encoding).

## 3. Fase 1: Exploración (Tarea 1)

Script auxiliar de exploración (puede vivir en `src/exploracion.py`, no forma parte de `main.py`).

1. Para cada año y cada tipo de archivo, leer solo `nrows=1000` (recorridos directamente desde el ZIP; si el ZIP trae más de un archivo, usar `zipfile` para listar y elegir el CSV correcto).
2. Detectar: nombre del archivo dentro del ZIP, separador (`,` o `;`), encoding, nombres de columnas, formato de fecha y hora, si trae duración y en qué unidad, si trae id de usuario.
3. Contar la **cantidad total de filas** de cada archivo (lectura por `chunksize`, sin cargar todo en memoria).
4. Para usuarios, ver `value_counts()` de género por año y rango de edades/fechas de alta.
5. Completar en `DECISIONES.md` la tabla:

| Aspecto a revisar | 2016 | 2019 | 2024 |
|---|---|---|---|
| Nombre del archivo dentro del ZIP | | | |
| Separador de columnas | | | |
| Nombres de las columnas | | | |
| Formato de fecha y hora | | | |
| ¿Trae duración? ¿En qué unidad? | | | |
| ¿Trae id de usuario? | | | |
| Cantidad total de filas | | | |

Y una tabla equivalente para los archivos de **usuarios**.

**Aceptación**: ambas tablas completas, con las cantidades de fila que se usarán para conciliar en bronze.

## 4. Fase 2: Bronze (Tarea 2) — `src/bronze.py`

Copia fiel, sin limpieza.

Requisitos:
- Leer con `pd.read_csv(..., dtype=str)`; los recorridos directo desde el ZIP (`compression="zip"` o `zipfile` si hay varios archivos). Usar el separador/encoding detectados por año (configuración por año, no código repetido).
- Leer por partes con `chunksize` (los archivos son enormes) y escribir cada año a **un único** `data.parquet` (usar `pyarrow.parquet.ParquetWriter` con esquema todo string, abriendo el archivo en modo escritura que reemplaza al existente).
- Preservar `keep_default_na=False` o equivalente si hace falta para no alterar valores vacíos; todo queda como texto. No renombrar, no convertir tipos, no eliminar filas.
- Agregar tres columnas de metadatos: `_archivo_origen` (nombre del archivo de origen, ej. `recorridos-2019.zip`), `_fecha_ingesta` (`datetime.now().isoformat(timespec="seconds")`, una sola vez por ejecución y por archivo), `_anio` (string).
- Salida:
```
data/bronze/recorridos/anio=2016/data.parquet
data/bronze/recorridos/anio=2019/data.parquet
data/bronze/recorridos/anio=2024/data.parquet
data/bronze/usuarios/anio=2016/data.parquet
data/bronze/usuarios/anio=2019/data.parquet
data/bronze/usuarios/anio=2024/data.parquet
```
- Función sugerida: `ingestar_archivo(ruta: Path, anio: int, destino: Path) -> int` (devuelve filas escritas) y `ejecutar_bronze() -> dict` (devuelve conteos por año/tipo para conciliar).

**Aceptación**:
- Filas en bronze por año = filas del archivo original (Tarea 1).
- Columnas = originales + las 3 de metadatos.
- Dos ejecuciones seguidas dejan los mismos conteos.

## 5. Fase 3: Silver (Tarea 3) — `src/silver.py`

Única capa que conoce las diferencias entre años. Lee **solo bronze**.

### 5.1 Esquema canónico

**silver/viajes**

| Columna | Tipo pandas |
|---|---|
| id_viaje | string |
| fecha_hora_inicio | datetime64[ns] |
| fecha_hora_fin | datetime64[ns] |
| duracion_seg | Int64 |
| id_estacion_origen | Int64 |
| nombre_estacion_origen | string |
| id_estacion_destino | Int64 |
| nombre_estacion_destino | string |
| id_usuario | string (puede ser nulo) |
| anio_fuente | Int64 |

**silver/usuarios**

| Columna | Tipo pandas |
|---|---|
| id_usuario | string |
| genero | string (F, M, OTRO, DESCONOCIDO) |
| edad | Int64 (puede ser nula) |
| fecha_alta | datetime64[ns] |
| anio_fuente | Int64 |

Usar `Int64` con mayúscula (nullable), nunca `int64`. Orden de columnas exactamente el de las tablas.

### 5.2 Pasos (en orden)

1. **Mapeo de columnas**: diccionarios `MAPEO_VIAJES` y `MAPEO_USUARIOS` por año (`{anio: {col_original: col_canonica}}`), todo lo que cambia entre años vive ahí. Función `renombrar(df, anio)` que renombra y descarta lo que no está en el mapeo. Si un año no trae una columna (ej. duración, id_usuario), no se incluye en el mapeo; se calcula o se crea nula en el paso siguiente. Además de los mapeos, una config por año con los **formatos de fecha**.
2. **Tipos y columnas faltantes**:
   - Fechas: `pd.to_datetime(..., format=<formato del año>, errors="coerce")`.
   - Ids de estación: `pd.to_numeric(..., errors="coerce").astype("Int64")`.
   - Duración: si el año no la trae o viene en otra unidad, calcular `(fecha_hora_fin - fecha_hora_inicio).dt.total_seconds()` y llevarla a `Int64` en segundos. Si viene en segundos, convertirla a `Int64`. Cuidar separadores de miles (ej. `"1.185"` en el ejemplo del enunciado) al parsear.
   - `anio_fuente` como `Int64` (desde `_anio`).
   - Si no hay `id_usuario` en un año, columna nula (`pd.NA`, dtype `string`).
3. **Normalización**:
   - Nombres de estación: `strip`, mayúsculas, espacios múltiples → uno solo (`" Pacifico "` == `"PACIFICO"`). Mantener nulos como nulos.
   - Género: mapear a `F`, `M`, `OTRO`, `DESCONOCIDO`. Base del mapeo: FEMALE/Female/F/Femenino → F; MALE/Male/M/Masculino → M; OTHER/Otro/X → OTRO; vacío, nulo o cualquier otro valor → DESCONOCIDO. Comparar en forma normalizada (strip + upper, sin tildes). **Completar el mapeo con los valores reales** que aparezcan en `value_counts()` y documentarlo en `DECISIONES.md`.
4. **Calidad y cuarentena de viajes** (`aplicar_calidad(df) -> (validos, cuarentena)`). Un registro que falla **no se borra**: va a `silver/viajes_cuarentena` con columna `motivo`. Si falla varias reglas, se guarda **solo el primer motivo en este orden**:

| Orden | Condición de rechazo | motivo |
|---|---|---|
| 1 | `fecha_hora_inicio` o `fecha_hora_fin` nulo (NaT) | FECHA_INVALIDA |
| 2 | `id_estacion_origen` o `id_estacion_destino` nulo | ESTACION_NULA |
| 3 | fin anterior al inicio | FIN_ANTERIOR_A_INICIO |
| 4 | duración > 86400 s | DURACION_MAYOR_24H |
| 5 | `id_viaje` ya apareció en una fila anterior | DUPLICADO |

   Implementar con `motivo.mask(motivo.isna() & condicion, nombre)` como sugiere el enunciado. La regla 5 usa `duplicated(keep="first")` y debe evaluarse **solo sobre filas que aún no tienen motivo**; definir y documentar si el chequeo de duplicados es global (entre años) o por año. Recomendado: **global**, porque `id_viaje` debe ser único en `silver/viajes`. Si el volumen impide hacerlo en memoria, hacerlo con un set de ids vistos acumulado entre años o con particionado; documentar la decisión. Un `id_viaje` nulo o vacío: decisión propia, documentar (sugerido: tratarlo como no válido con el motivo más cercano, o agregar un motivo extra al final y documentarlo).
5. **Usuarios**: reglas mínimas: `id_usuario` no nulo; sin duplicados (si un usuario aparece más de una vez, quedarse con el de `fecha_alta` más reciente; si empata o la fecha es NaT, criterio estable y documentado). Edad < 16 o > 90 → **se reemplaza por nulo**, no se rechaza; anotarlo en `DECISIONES.md`. Los usuarios rechazados por `id_usuario` nulo se guardan en `silver/usuarios_cuarentena` con `motivo` (decisión propia, documentarla). Un `id_usuario` puede repetirse entre años: el dedup es global.
6. **Guardar y verificar**:
   - Guardar `silver/viajes`, `silver/viajes_cuarentena`, `silver/usuarios` en Parquet (rutas fijas, reemplazo total).
   - Función `validar_esquema(df, esquema)` que compare `df.dtypes` y el orden de columnas contra un diccionario de tipos esperados y lance excepción si algo no coincide. **Llamarla siempre antes de guardar.**
   - **Conciliación** por año: `filas_bronze == filas_silver + filas_cuarentena`. Si no cierra, el pipeline falla con error explícito. Hacerlo también para usuarios, contemplando la regla de dedup (documentar cómo se concilia: filas descartadas por dedup cuentan como cuarentena o se reportan aparte).
   - `id_viaje` único en `silver/viajes`.

Manejo de memoria: procesar por año (y por chunks si hace falta), sin cargar los tres años de bronze completos si la máquina no alcanza; documentar el enfoque.

**Aceptación**: dtypes idénticos a los del enunciado; conciliación OK por año; `id_viaje` único; cuarentena con motivos del conjunto permitido; dos ejecuciones dan los mismos conteos.

## 6. Fase 4: Gold (Tarea 4) — `src/gold.py`

Modelo estrella desde **silver**. Claves naturales (sin claves sustitutas). **Sin lógica de limpieza**: si hay que corregir algo, pertenece a silver.

**dim_fecha**: una fila por cada día del período cubierto por los datos (rango continuo de `min` a `max` de `fecha_hora_inicio`).
`fecha_key` (int AAAAMMDD), `fecha`, `anio`, `mes`, `dia`, `dia_semana` (nombre en español: lunes…domingo, **sin depender del locale del sistema**, usar un mapa fijo), `es_fin_de_semana` (bool: sábado/domingo).

**dim_estacion**: una fila por estación, desde estaciones de origen y destino unidas. `id_estacion`, `nombre_estacion` (el nombre normalizado del **año más reciente** en que aparece), `primer_anio`, `ultimo_anio`.

**dim_usuario**: desde `silver/usuarios`. `id_usuario`, `genero`, `edad`, `rango_etario` (`16-25`, `26-35`, `36-50`, `51+`, `DESCONOCIDO` si la edad es nula), `anio_alta`.

**fact_viajes**: una fila por viaje de `silver/viajes`. `id_viaje`, `fecha_key` (fecha de inicio), `hora_inicio` (0-23), `id_estacion_origen`, `id_estacion_destino`, `id_usuario` (puede ser nulo), `duracion_min` (minutos con **un decimal**), `es_circular` (origen == destino). Agregar `anio` (de `anio_fuente`) solo si se justifica en `DECISIONES.md`; los reportes por año se pueden derivar de `dim_fecha` vía `fecha_key`.

Guardar cada tabla en Parquet en `data/gold/` (por ejemplo `data/gold/fact_viajes.parquet`).

**Verificaciones (ejecutadas por código, con falla explícita)**:
- filas de `fact_viajes` == filas de `silver/viajes`.
- todo `fecha_key` de fact existe en `dim_fecha`; todo id de estación (origen y destino) existe en `dim_estacion`.
- Contar viajes cuyo `id_usuario` **no** está en `dim_usuario` (no es error); anotar el número y una posible explicación en `DECISIONES.md` (ej.: 2016 no trae id de usuario; usuarios dados de baja o no incluidos en el archivo de usuarios de ese año; archivos de usuarios con cortes distintos).

## 7. Fase 5: Reportes (Tarea 5) — `src/reportes.py`

Generar desde **gold** (nunca bronze/silver, salvo el reporte 5). CSV en `data/gold/reportes/`, UTF-8, separador coma, **punto decimal y sin separador de miles** (los puntos de miles del enunciado son solo ilustrativos), con encabezados exactos:

1. `viajes_por_mes.csv`: `anio, mes, cantidad_viajes`.
2. `top10_estaciones_por_anio.csv`: `anio, ranking, nombre_estacion, cantidad_viajes` (10 estaciones con más viajes **iniciados** por año; desempate determinista y documentado, ej. por nombre). Comentar en `DECISIONES.md` si el top cambia entre años.
3. `duracion_por_rango_y_genero.csv`: `rango_etario, genero, cantidad_viajes, duracion_promedio_min`. Los viajes sin usuario o con usuario no encontrado deben aparecer con `DESCONOCIDO` en ambas dimensiones (left join), y se documenta.
4. `viajes_circulares.csv`: `anio, total_viajes, viajes_circulares, porcentaje_circulares` (porcentaje con 1-2 decimales).
5. `calidad_por_anio.csv`: `anio, filas_bronze, filas_silver, filas_cuarentena, porcentaje_cuarentena` (**excepción**: se arma con conteos de bronze, silver y cuarentena; contar leyendo solo metadatos/columna mínima de Parquet para no cargar todo).

Escribir los CSV con ruta fija (reemplazo). Ordenar de forma determinista para que dos ejecuciones sean byte a byte idénticas.

## 8. `main.py`

- Ejecuta en orden: bronze → silver → gold → reportes, cada uno como función `ejecutar_*()` importada de `src/`.
- Valida al inicio que existan los 6 archivos de `data/raw/` y los directorios de salida (crearlos si no existen).
- Logging simple por etapa (`logging`, no `print` suelto) con conteos y tiempos.
- Cualquier falla de verificación corta la ejecución con código de salida distinto de 0.
- Opcional: argumento `--desde bronze|silver|gold|reportes` para reprocesar desde una capa, sin romper el comportamiento por defecto (pipeline completo).

## 9. Entregables

- `src/` + `main.py` funcionando con `python main.py`.
- `README.md`: versión de Python, `pip install -r requirements.txt`, **dónde colocar los archivos descargados** (`data/raw/`), nombres esperados, cómo ejecutar, estructura del proyecto, y estimación de memoria/tiempo.
- `requirements.txt`: `pandas`, `pyarrow` (con versiones mínimas).
- `DECISIONES.md` con: (a) tablas de diferencias entre años (recorridos y usuarios); (b) mapeo de columnas por año y mapeo de valores de género realmente usado; (c) cada decisión no indicada en el enunciado con una línea de justificación (dedup de `id_viaje`, tratamiento de ids nulos, edades fuera de rango, usuarios sin match, desempates en top 10, formato numérico de los CSV, etc.).
- Los cinco CSV en `data/gold/reportes/`.
- No incluir `data/raw`, `data/bronze`, `data/silver` ni tablas de gold.

## 10. Checklist final de aceptación

- [ ] `python main.py` corre de punta a punta desde un `data/` con solo `raw/` poblado.
- [ ] Segunda ejecución: mismos conteos y mismos reportes.
- [ ] Bronze: filas por año = filas originales; columnas originales + 3 metadatos; todo string.
- [ ] Silver: dtypes exactos; conciliación `bronze = silver + cuarentena` por año; `id_viaje` único; motivos válidos y en orden de prioridad.
- [ ] Gold: filas `fact_viajes == silver/viajes`; integridad referencial con `dim_fecha` y `dim_estacion`; conteo de usuarios huérfanos documentado.
- [ ] Reportes 1 a 4 se generan solo desde gold; el 5 desde conteos de bronze/silver/cuarentena.
- [ ] Todo el código en funciones; ninguna lógica de limpieza en gold; ninguna dependencia de pasos manuales.
- [ ] `README.md`, `DECISIONES.md` y `requirements.txt` completos.
