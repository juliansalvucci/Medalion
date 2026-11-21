# Pipeline Ecobici — Bronze / Silver / Gold

Pipeline de datos de las bicicletas publicas de Buenos Aires
(https://data.buenosaires.gob.ar/dataset/bicicletas-publicas) organizado en las
tres capas de una arquitectura medallion:

- **Bronze**: copia fiel de los archivos originales, sin limpieza (todo texto).
- **Silver**: unico esquema canonico; conoce las diferencias entre anos, aplica
  normalizacion, reglas de calidad y cuarentena.
- **Gold**: modelo estrella (dimensiones + hechos) sin logica de limpieza.
- **Reportes**: los 5 CSV de entrega, generados desde gold (el 5to usa
  metadatos de bronze/silver).

---

## 1. Requisitos

- **Python 3.11 o superior** (probado con Python 3.11.3).
- Dependencias: `pandas` y `pyarrow`.

```bash
python -m venv .venv
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

En Linux/macOS: `source .venv/bin/activate`.

No se usa Spark, Polars ni DuckDB: el pipeline corre solo con pandas + pyarrow.

---

## 2. Donde poner los archivos descargados

Colocar los **6 archivos** en `data/raw/`:

| Ano | Recorridos (viajes) | Usuarios |
|---|---|---|
| 2016 | `recorridos-realizados-2016.zip` | `usuarios-ecobici-2016.csv` |
| 2019 | `recorridos-realizados-2019.zip` | `usuarios_ecobici_2019.csv` |
| 2024 | `recorridos-realizados-2024.zip` | `usuarios_ecobici_2024.csv` |

Notas:

- Los **nombres exactos los define usted al descargarlos**: el codigo detecta
  los archivos por **patron** (el nombre contiene el ano, y `recorrido` o
  `usuario`), asi que `recorridos-realizados-2019.zip` o `trips-2019.zip` se
  detectan igual.
- Los recorridos se entregan como **ZIP** (el CSV vive dentro del ZIP; el
  codigo usa `zipfile` para elegir el CSV correcto si el ZIP trae varios o si
  el nombre interno no es el esperado: nunca falla por nombre).
- Los usuarios van en **CSV** (no XLSX).
- Si falta algun archivo, `python main.py` se detiene con un mensaje claro
  indicando que falta. **Nunca descargue ni descomprima por su cuenta dentro
  de `data/raw/`**: esa carpeta es de solo lectura.
- Tamano observado de la fuente: ~570 MB en `data/raw/` (ZIPs de recorridos:
  11,5 / 379,2 / 168,1 MB para 2016 / 2019 / 2024; CSVs de usuarios:
  1,3 / 3,4 / 6,9 MB).

---

## 3. Como ejecutar

```bash
python main.py
```

Ejecuta **bronze -> silver -> gold -> reportes** de punta a punta. Cualquier
verificacion que no cierra (conteos, dtypes, integridad referencial) corta la
ejecucion con **codigo de salida distinto de 0**.

Para reprocesar desde una capa (sin correr todo de nuevo):

```bash
python main.py --desde silver     # silver -> gold -> reportes
python main.py --desde gold       # gold -> reportes
python main.py --desde reportes   # solo los CSV
```

Es **idempotente**: cada salida se escribe en una ruta fija y reemplaza la
anterior (nunca hace append). Correr dos veces deja los mismos conteos y los
mismos CSV **byte a byte**.

La Fase 1 (exploracion) es un script auxiliar que no forma parte del
pipeline:

```bash
python src/exploracion.py
```

---

## 4. Estructura del proyecto

```
ecobici-pipeline/
├── data/
│   ├── raw/                     # ZIP/CSV descargados, solo lectura
│   ├── bronze/
│   │   ├── recorridos/anio={2016,2019,2024}/data.parquet
│   │   └── usuarios/anio={2016,2019,2024}/data.parquet
│   ├── silver/
│   │   ├── viajes.parquet
│   │   ├── viajes_cuarentena.parquet
│   │   ├── usuarios.parquet
│   │   └── usuarios_cuarentena.parquet
│   └── gold/
│       ├── dim_fecha.parquet
│       ├── dim_estacion.parquet
│       ├── dim_usuario.parquet
│       ├── fact_viajes.parquet
│       └── reportes/            # <- los 5 CSV entregables
│           ├── viajes_por_mes.csv
│           ├── top10_estaciones_por_anio.csv
│           ├── duracion_por_rango_y_genero.csv
│           ├── viajes_circulares.csv
│           └── calidad_por_anio.csv
├── src/
│   ├── __init__.py
│   ├── config.py                # rutas, deteccion de archivos, constantes
│   ├── exploracion.py           # Fase 1 (auxiliar, no va en main.py)
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   └── reportes.py
├── main.py
├── DECISIONES.md                # todas las decisiones justificadas
├── README.md
├── requirements.txt
└── .gitignore                   # excluye raw/bronze/silver/gold salvo reportes/
```

### Capas y que lee cada una

| Etapa | Lee | Escribe |
|---|---|---|
| bronze | `data/raw/` | `data/bronze/` |
| silver | `data/bronze/` | `data/silver/` |
| gold | `data/silver/` | `data/gold/` |
| reportes | `data/gold/` (el 5to: metadatos de bronze/silver) | `data/gold/reportes/` |

---

## 5. Estimacion de memoria y tiempo

Medido en una maquina Windows con **7,8 GB de RAM** (aprox. 2,2 GB libres al
iniciar) y Python 3.11:

| Etapa | Tiempo (corrida de referencia) | Salida aprox. |
|---|---|---|
| bronze | ~73 s | 416 MB (6 Parquet) |
| silver | ~119 s | 324 MB (4 Parquet) |
| gold | ~155 s | 139 MB (4 Parquet) |
| reportes | ~8 s | 5 CSV |
| **Total** | **~356 s (6 min)** | |

(Entre corridas osciló entre 356 y 392 s según el uso de la maquina.)

- **Volumen**: 10.373.095 viajes y 320.158 usuarios en bruto; silver deja
  10.266.320 viajes y 314.927 usuarios validos.
- **Memoria**: pico de RSS del proceso medido entre **2,1 y 2,7 GB**. Se
  mantiene bajo control porque silver y gold procesan por **lotes (row groups
  de Parquet)** en vez de cargar los 10 millones de filas; los archivos de
  usuarios si se cargan completos (son de hasta 197.077 filas). En la maquina
  de prueba (7,8 GB de RAM total) la disponible llego a bajar a ~0,1 GB, por lo
  que conviene tener **al menos 4 GB de RAM libres** al inicio (idealmente 8 GB
  en total).
- El paso mas lento es gold (dos pasadas sobre silver para armar fact y
  verificar integridad).

---

## 6. Verificaciones automaticas

El pipeline falla con mensaje explicito si algo no cierra:

- **Bronze**: filas por ano = filas del archivo original; columnas originales
  + las 3 de metadatos (`_archivo_origen`, `_fecha_ingesta`, `_anio`);
  todo string.
- **Silver**: `validar_esquema()` antes de cada guardado (dtypes y orden
  exactos, `Int64` nullable); conciliacion por ano
  `filas_bronze == filas_silver + filas_cuarentena`; `id_viaje` unico;
  motivos de cuarentena en el orden de prioridad de la SPEC.
- **Gold**: `filas_fact_viajes == filas_silver_viajes`; todo `fecha_key`
  existe en `dim_fecha`; todo id de estacion existe en `dim_estacion`;
  viajes con `id_usuario` fuera de `dim_usuario` se cuentan y documentan
  (no es error).
- **Reportes**: encabezados exactos, orden determinista y UTF-8.

Los resultados de las corridas (conciliaciones, motivos de cuarentena,
conteos) estan tabulados en **[DECISIONES.md](DECISIONES.md)**.

---

## 7. Entregables

Lo que se entrega es `src/`, `main.py`, `README.md`, `DECISIONES.md`,
`requirements.txt` y los **5 CSV de `data/gold/reportes/`**. El `.gitignore`
excluye `data/raw/`, `data/bronze/`, `data/silver/` y `data/gold/*` salvo
`data/gold/reportes/`.
