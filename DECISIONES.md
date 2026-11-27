# DECISIONES.md — Pipeline Ecobici (Bronze / Silver / Gold)

Documento de decisiones del proyecto. Cada apartado de la SPEC que no viene
indicado en el enunciado queda justificado aqui con una linea de razon.

---

## (a) Fase 1: diferencias entre los archivos de cada anio

### Recorridos (viajes)

| Aspecto a revisar | 2016 | 2019 | 2024 |
|---|---|---|---|
| Nombre del archivo dentro del ZIP | `recorridos-realizados-2016.csv` | `trips_2019.csv` | `badata_ecobici_recorridos_realizados_2024.csv` |
| Separador de columnas | `,` (encoding `utf-8-sig`) | `,` (encoding `utf-8-sig`) | `,` (encoding `utf-8-sig`) |
| Nombres de las columnas | `periodo`, `genero_usuario`, `fecha_origen_recorrido`, `id_estacion_origen`, `nombre_estacion_origen`, `long_estacion_origen`, `lat_estacion_origen`, `domicilio_estacion_origen`, `duracion_recorrido`, `fecha_destino_recorrido`, `id_estacion_destino`, `nombre_estacion_destino`, `long_estacion_destino`, `lat_estacion_destino`, `domicilio_estacion_destino` | `Unnamed: 0`, `Id_recorrido`, `duracion_recorrido`, `fecha_origen_recorrido`, `id_estacion_origen`, `nombre_estacion_origen`, `direccion_estacion_origen`, `long_estacion_origen`, `lat_estacion_origen`, `fecha_destino_recorrido`, `id_estacion_destino`, `nombre_estacion_destino`, `direccion_estacion_destino`, `long_estacion_destino`, `lat_estacion_destino`, `id_usuario`, `modelo_bicicleta`, `género` | `id_recorrido`, `duracion_recorrido`, `fecha_origen_recorrido`, `id_estacion_origen`, `nombre_estacion_origen`, `direccion_estacion_origen`, `long_estacion_origen`, `lat_estacion_origen`, `fecha_destino_recorrido`, `id_estacion_destino`, `nombre_estacion_destino`, `direccion_estacion_destino`, `long_estacion_destino`, `lat_estacion_destino`, `id_usuario`, `modelo_bicicleta`, `genero` |
| Formato de fecha y hora | `%Y-%m-%d %H:%M:%S` (origen y destino) | `%Y-%m-%d %H:%M:%S` (origen y destino) | `%Y-%m-%d %H:%M:%S` (origen y destino) |
| ¿Trae duración? ¿En qué unidad? | Si: `duracion_recorrido` como **texto timedelta** (`0 days 00:16:29.000000000`) | Si: `duracion_recorrido` en **segundos** con separador de miles por coma (`2,814` = 2814 s) | Si: `duracion_recorrido` en **segundos** enteros (`568`) |
| ¿Trae id de usuario? | **No** (solo trae `genero_usuario`) | Si: `id_usuario` (viene con el sufijo `BAEcobici`) | Si: `id_usuario` (en formato float: `992557.0`) |
| ¿Trae id de viaje? | **No trae ninguna columna de id de viaje** | Si: `Id_recorrido` (con sufijo `BAEcobici`) | Si: `id_recorrido` |
| Cantidad total de filas | 596.807 | 6.217.004 | 3.559.284 |

### Usuarios

| Aspecto a revisar | 2016 | 2019 | 2024 |
|---|---|---|---|
| Nombre del archivo | `usuarios-ecobici-2016.csv` | `usuarios_ecobici_2019.csv` | `usuarios_ecobici_2024.csv` |
| Separador de columnas | `,` (encoding `utf-8-sig`) | `,` (encoding `utf-8-sig`) | `,` (encoding `utf-8-sig`) |
| Nombres de las columnas | `id_usuario`, `genero_usuario`, `edad_usuario`, `fecha_alta`, `hora_alta` | `ID_usuario`, `genero_usuario`, `edad_usuario`, `fecha_alta`, `hora_alta` | `id_usuario`, `genero_usuario`, `edad_usuario`, `fecha_alta`, `hora_alta` |
| Formato de fecha y hora | `fecha_alta` = `%d-%m-%y` (2 digitos de anio) + `hora_alta` = `%I:%M:%S %p` | `fecha_alta` = `%Y-%m-%d` + `hora_alta` = `%H:%M:%S` | `fecha_alta` = `%Y-%m-%d` + `hora_alta` = `%H:%M:%S` |
| Genero (`value_counts`) | `M`=21.697, `F`=17.784 | `MALE`=41.107, `FEMALE`=24.285, `OTHER`=17.351, vacio=857 | `MALE`=88.655, `FEMALE`=79.310, `OTHER`=29.112 |
| Edad | min=16, max=88, sin nulos | min=0, max=935, 765 valores no numericos (31 menores de 16 y 15 mayores de 90) | min=4, max=2021, sin nulos (9 menores de 16 y 85 mayores de 90) |
| Fechas de alta | de 2016-01-01 a 2016-12-31 | de 2019-02-16 a 2019-12-31 (83.600 en 2019; el corte de altas arranca en febrero) | de 2024-01-01 a 2024-12-31 (197.077 en 2024) |
| ¿Trae id de usuario? | Si: `id_usuario` | Si: `ID_usuario` (mayusculas) | Si: `id_usuario` |
| Cantidad total de filas | 39.481 | 83.600 | 197.077 |

Las cantidades de fila de arriba son las que se usan para conciliar en bronze
(coiniciden exactamente con los conteos de `data/bronze/`).

---

## (b) Mapeos por anio y mapeo de genero

### `MAPEO_VIAJES` (`{anio: {columna_original: columna_canonica}}`)

Lo que no aparece en el mapeo se **descarta** en silver (nunca se renombra ni
se inventa una columna).

```python
2016: {   # NO trae id_recorrido ni id_usuario
          # "duracion_recorrido" se mapea pero NO se usa: la duracion se
          # calcula desde las fechas (decision 8)
    "fecha_origen_recorrido": "fecha_hora_inicio",
    "fecha_destino_recorrido": "fecha_hora_fin",
    "duracion_recorrido": "duracion_seg",
    "id_estacion_origen": "id_estacion_origen",
    "nombre_estacion_origen": "nombre_estacion_origen",
    "id_estacion_destino": "id_estacion_destino",
    "nombre_estacion_destino": "nombre_estacion_destino",
}
2019: {
    "Id_recorrido": "id_viaje",          # mayusculas distintas a 2024
    "duracion_recorrido": "duracion_seg",
    "fecha_origen_recorrido": "fecha_hora_inicio",
    "fecha_destino_recorrido": "fecha_hora_fin",
    "id_estacion_origen": "id_estacion_origen",
    "nombre_estacion_origen": "nombre_estacion_origen",
    "id_estacion_destino": "id_estacion_destino",
    "nombre_estacion_destino": "nombre_estacion_destino",
    "id_usuario": "id_usuario",
    # "Unnamed: 0" NO se mapea: en el archivo el primer campo del header
    # esta **vacio** (`""`) y es **pandas** quien le pone ese nombre. Es un
    # indice residual del CSV, no una columna de datos
}
2024: {
    "id_recorrido": "id_viaje",          # minusculas
    ... igual que 2019 ...
}
```

### `MAPEO_USUARIOS`

```python
2016: {"id_usuario": "id_usuario", "genero_usuario": "genero",
       "edad_usuario": "edad", "fecha_alta": "fecha_alta", "hora_alta": "hora_alta"}
2019: {"ID_usuario": "id_usuario", ...}   # la unica diferencia es "ID_usuario"
2024: {"id_usuario": "id_usuario", ...}   # identico a 2016
```

### Configuracion por anio (formatos y unidades)

| Config | 2016 | 2019 | 2024 |
|---|---|---|---|
| `FORMATO_FECHA_VIAJES` | `%Y-%m-%d %H:%M:%S` | `%Y-%m-%d %H:%M:%S` | `%Y-%m-%d %H:%M:%S` |
| `FORMATO_FECHA_USUARIOS` (fecha, hora) | `%d-%m-%y`, `%I:%M:%S %p` | `%Y-%m-%d`, `%H:%M:%S` | `%Y-%m-%d`, `%H:%M:%S` |
| `CONFIG_DURACION` | `desde_fechas` (la fuente trae texto timedelta, se calcula `fin - inicio`) | `segundos_separador_miles` | `segundos` |
| `CONFIG_IDS` | sin particularidades | sufijo `BAEcobici` en todos los id | `id_usuario` en formato float (`.0`) |
| `CONFIG_LECTURA` (bronze) | sep `,`, `utf-8-sig` | sep `,`, `utf-8-sig` | sep `,`, `utf-8-sig` |

### Mapeo de valores de genero realmente usado (`MAPEO_GENERO`)

Se aplico `strip + mayusculas + sin tildes` antes de mapear. Valores observados
en los 6 archivos y su destino:

| Valor observado (normalizado) | Aparece en | Resultado |
|---|---|---|
| `M` | usuarios 2016 (21.697) | `M` |
| `F` | usuarios 2016 (17.784) | `F` |
| `MALE` | usuarios 2019 (41.107) y 2024 (88.655) | `M` |
| `FEMALE` | usuarios 2019 (24.285) y 2024 (79.310) | `F` |
| `OTHER` | usuarios 2019 (17.351) y 2024 (29.112) | `OTRO` |
| `""` (vacio) | usuarios 2019 (857) | `DESCONOCIDO` |
| cualquier otro / nulo | no observado, por red | `DESCONOCIDO` |

Tambien estan mapeados por si aparecen: `FEMENINO`→`F`, `MASCULINO`→`M`,
`OTRO`/`X`→`OTRO`.

---

## (c) Decisiones propias (no indicadas en el enunciado)

### Datos e ids

1. **`id_viaje` sintetico en 2016** — 2016 no trae ninguna columna de id de
   viaje; se crea `2016-<posicion_de_fila_en_el_archivo_original>` (determinista
   entre ejecuciones) para no tirar 503.252 viajes validos a la basura.
   *(Decision tomada por el usuario de este repositorio ante la opcion que la
   SPEC deja como "decision propia".)*
2. **Motivo extra `ID_VIAJE_NULO`** — la SPEC sugiere agregar un motivo al
   final si el `id_viaje` es nulo o vacio; queda como regla 6 (la ultima) por
   si alguna futura fuente trae ids vacios. Con los datos actuales no se
   dispara (0 filas).
3. **Regla DUPLICADO global entre anios** — se acumula un `set` de ids vistos
   entre lotes y entre anios (recomendado por la SPEC), porque `id_viaje` debe
   ser unico en `silver/viajes`. Se verifico al final leyendo solo la columna
   `id_viaje` del Parquet: unicos los 10.266.320 ids.
4. **La regla 5 solo se evalua sobre filas sin motivo previo y con id no nulo**
   — asi una fila rechazada por fecha invalida no "contamina" el set de ids
   vistos ni un id nulo se marca DUPLICADO.
5. **Sufijo `BAEcobici` en los id de 2019** — el CSV trae `5BAEcobici`,
   `9379BAEcobici`, `6243BAEcobici`, etc. En silver se elimina el sufijo antes
   de convertir; si no, `pd.to_numeric` dejaria todas las estaciones nulas y el
   100% de 2019 (6.217.004 filas) iria a cuarentena.
6. **`id_usuario` float en 2024** — el archivo trae `992557.0`; se quita el
   `.0` para que cruce con el archivo de usuarios (`992557`).
7. **`Unnamed: 0` de 2019 se descarta** — es un indice residual del CSV, no un
   dato; no esta en el mapeo.

### Fechas y duraciones

8. **Duracion 2016 = `fin - inicio`** — el enunciado (Tarea 3, Paso 3) pide
   calcular la duracion cuando el anio no la trae o viene en "otra unidad";
   2016 la trae como **texto timedelta** (`0 days 00:16:29.000000000`), que
   es otra representacion, asi que `CONFIG_DURACION[2016] = "desde_fechas"`
   ignora ese valor y usa `(fecha_hora_fin - fecha_hora_inicio)`.
   **Por que no parsear el texto: en 2016 la columna no mide lo mismo que las
   fechas** (medido sobre los 503.252 viajes de 2016 con destino informado):
   - correlacion entre `duracion_recorrido` y `fin - inicio`: **0,0024**
     (Spearman 0,0037); solo el **0,35 %** de los viajes coincide dentro de
     ±5 s y el **8,64 %** dentro de ±10 %;
   - las diferencias van de **-10.149 a +10.171 s** (mediana +195 s, desvio
     2.228 s): no es un error de unidad, porque eso daria un **factor fijo**
     (el ratio `fin - inicio` / fuente recorre de 0,06 a 17,6);
   - control en los otros anios: en **2019 y 2024 la columna coincide
     exactamente** con `fin - inicio` (correlacion 1,0000; 100 % dentro de
     ±5 s), o sea que la regla del enunciado reproduce la propia columna de
     la fuente en esos anios;
   - las dos columnas estan acotadas al mismo rango (p0 = 10,00 min y
     p100 = 180 min en ambas): los valores parecen duraciones reales de la
     misma poblacion, pero **no estan emparejados con las fechas de su fila**.
   Efecto medido: los conteos NO cambian con ninguna de las dos lecturas
   (2016 sigue con 93.555 filas en cuarentena, todas `FECHA_INVALIDA`, y
   503.252 validas; ningun viaje supera las 24 h ni termina antes de
   empezar), pero si cambian los promedios del reporte 3: la fila
   `DESCONOCIDO,DESCONOCIDO` pasa de 28,3 a 28,6 min, porque **una parte**
   de sus viajes es de 2016: 503.252 de 6.650.991 (7,6 %), todos sin
   usuario. La duracion promedio de 2016 sube 3,23 min al calcularla desde
   las fechas (35,88 -> 39,11 min), y ese 7,6 % mueve el promedio de la fila
   0,076 x 3,23 = 0,24 min, que redondeado a un decimal da el 28,3 -> 28,6
   observado. Usar la columna de la fuente
   dejaria la duracion de 2016 inconsistente con las fechas que aplican las
   reglas de calidad, y lo dejaria ademas en contra de 2019/2024, donde la
   columna es igual a `fin - inicio`.
9. **Separadores de miles en la duracion** — en 2019 se quitan `.` y `,` solo
   cuando separan miles (`"1.185"`→1185, `"2,814"`→2814), sin tocar enteros.
10. **Duracion no parseable -> calculada desde las fechas** — asi la regla
    `DURACION_MAYOR_24H` nunca ve duraciones nulas.
11. **`fecha_alta` = fecha + `hora_alta`** con respaldo a solo la fecha si la
    hora no parsea — el esquema canonico solo tiene `fecha_alta`, pero la hora
    sirve para desempatar el dedup de usuarios.
12. **Fechas de viaje en `datetime64[ns]` explicito** — pandas 3 devuelve
    `datetime64[us]` al parsear; se hace `.astype("datetime64[ns]")` para
    cumplir el esquema de la SPEC.

### Calidad y cuarentena

13. **Nada se borra**: los rechazados van a `silver/viajes_cuarentena` con
    `motivo` (y `silver/usuarios_cuarentena` para usuarios), en ese orden de
    prioridad: `FECHA_INVALIDA`, `ESTACION_NULA`, `FIN_ANTERIOR_A_INICIO`,
    `DURACION_MAYOR_24H`, `DUPLICADO`, `ID_VIAJE_NULO`.
14. **Usuarios con `id_usuario` nulo** -> cuarentena con motivo
    `ID_USUARIO_NULO` (la SPEC pide documentarlo; en estos datos son 0 filas).
15. **Dedup global de usuarios** — gana el registro con `fecha_alta` mas
    reciente; empates o `NaT` se resuelven: (1) gana el `anio_fuente` mas
    reciente, (2) si sigue empando se conserva la primera aparicion en el orden
    de lectura (2016 → 2019 → 2024). El `NaT` se ordena como 1900-01-01
    (pierde frente a cualquier fecha real).
16. **Los descartados por dedup cuentan como cuarentena** (motivo
    `DUPLICADO_USUARIO`) para que la conciliacion `bronze = silver + cuarentena`
    cierre exacta en usuarios.
17. **Edades fuera de 16-90 -> nulo** (no se rechaza el usuario): 2019 deja 811
    edades nulas y 2024 deja 94 (valores imposibles como 0, 935 o 2021).
18. **Nombres de estacion**: `strip` + mayusculas + espacios multiples a uno
    solo (`" Pacifico "` == `"PACIFICO"`); nulos se conservan como nulos. No
    se toca la puntuacion: la fuente trae `131- HOSPITAL DE CLINICAS` sin
    espacio antes del guion mientras el resto de las estaciones usan
    `131 - ...`, y esa inconsistencia se conserva tal cual (el id de estacion
    es la clave, el nombre es solo el rotulo de la fuente del anio mas
    reciente). Las etiquetas de error de la fuente (`ETIQUETAS_ERROR`) pasan a
    nulo en esta misma normalizacion (decision 33).

### Gold

19. **`dim_fecha` en `int64` y claves de dim en `int64`** — las dimensiones no
    traen nulos, por lo que usan enteros normales (en silver si se usa el
    `Int64` nullable que pide la SPEC).
20. **`dim_estacion.nombre_estacion`** se elige con criterio determinista: anio
    mas reciente primero, y dentro del mismo anio el no nulo; desempate por
    nombre. `primer_anio`/`ultimo_anio` son min/max de `anio_fuente`.
21. **Viajes sin usuario en `fact_viajes`** — `id_viaje` de 2016 no trae
    usuario, por lo que esos viajes quedan con `id_usuario` nulo y aparecen
    como `DESCONOCIDO` en el reporte 3 (left join). Conteo en el apartado de
    verificaciones mas abajo.
22. **`fact_viajes` se escribe por lotes** (dos pasadas sobre
    `silver/viajes`) para no cargar 10 millones de filas en una maquina con
    poca RAM; las verificaciones de integridad se hacen lote por lote.

### Reportes

23. **`anio`/`mes` de los reportes se derivan de `dim_fecha`** (via
    `fecha_key`), tal como permite la SPEC; `fecha_key` ya esta verificado
    contra `dim_fecha` en gold.
24. **Desempate del top 10**: a igual cantidad de viajes ordena por nombre de
    estacion ascendente (determinista). Los nombres nulos usan el rotulo fijo
    `SIN NOMBRE` para que el orden no dependa de valores faltantes.
25. **El reporte 5 (`calidad_por_anio`) es solo de viajes** — sus columnas no
    distinguen tipo de tabla; el estado de usuarios se reporta en este
    documento y en los logs.
26. **Formato numerico de los CSV**: punto decimal y sin separador de miles,
    UTF-8, separador coma y fin de linea `\n`; los floats ya vienen redondeados
    (1 o 2 decimales) y todas las filas tienen orden fijo, asi que dos
    ejecuciones dejan archivos byte a byte identicos.
27. **El reporte 3 redondea el promedio a 1 decimal** y suma viajes sin
    usuario con `DESCONOCIDO` en ambas dimensiones.

### Ingenieria

28. **`_fecha_ingesta` tiene un unico valor por ejecucion** (no por archivo)
    para que los 6 archivos de bronze compartan el mismo sello de tiempo.
29. **Separador/encoding por anio en `CONFIG_LECTURA`** (detectados en la Fase
    1) con deteccion automatica de respaldo si falta algun valor: asi no hay
    codigo repetido por archivo.
30. **Procesamiento por lotes en silver y gold** (row groups de Parquet) y
    usuarios en memoria completa (320.158 filas): los viajes son 10 millones de
    filas y la maquina tiene 7.8 GB de RAM.
31. **Metadata de pandas en el esquema de los Parquet escritos con esquema
    explicito** — ``pq.ParquetWriter(ruta, <esquema>)`` usa el esquema que
    recibe en el constructor; si no trae metadata de pandas, ``pd.read_parquet``
    devuelve ``int64``/``str`` en vez de ``Int64``/``string`` (los dtypes del
    enunciado) en columnas sin nulos. El esquema se construye con
    ``_esquema_con_metadata()`` / ``_esquema_fact()`` (metadata generado desde
    un frame de referencia con los mismos dtypes), de modo que ``silver/viajes``,
    ``silver/viajes_cuarentena`` y ``gold/fact_viajes`` releen exactamente con
    el esquema que ``validar_esquema()`` aprobo antes de guardar.
32. **El pipeline no depende de pasos manuales**: `python main.py` valida
    `data/raw/`, crea los directorios de salida y ejecuta bronze → silver →
    gold → reportes; cualquier verificacion que no cierra corta con codigo de
    salida distinto de 0.
33. **Las etiquetas de error de la fuente se anulan en silver**: la SPEC
    pide que `dim_estacion.nombre_estacion` sea el nombre del **anio mas
    reciente**, pero el archivo de 2019 trae `ERRONEA` para el id 1 y
    `KP1808000368` para el id 10 (un marcador de error literal y un codigo
    interno; ninguno de los dos identifica un lugar). La limpieza se hace en
    **silver**, que es donde vive el resto de la normalizacion, y **no** en
    gold: `normalizar_nombre` deja esas dos etiquetas en **nulo**
    (`ETIQUETAS_ERROR` en `src/silver.py`), junto con el strip, las
    mayusculas y los espacios. Como `construir_dim_estacion` (gold, sin
    logica de limpieza) toma el **primer nombre no nulo** del anio mas
    reciente, el id 1 queda como `FACULTAD DE DERECHO` (12.600 viajes en 2016,
    direccion *Av. Figueroa Alcorta y Juan A. Bibiloni*) y el id 10 como
    `PUERTO MADERO - UCA` (12.377 viajes, *Moreau de Justo 1500*), y ninguna
    otra estacion cambia. No genera filas en cuarentena (el motivo
    `ESTACION_NULA` mira los ids de estacion, no los nombres) ni altera
    ningun conteo: solo se pierde un rotulo que no era un nombre.
    `ultimo_anio` sigue en 2019, porque la estacion aparece ese anio aunque
    su etiqueta quede en nulo. Si un id solo trajera etiquetas de error
    quedaria sin nombre (el reporte 2 lo mostraria como `SIN NOMBRE`); en los
    datos reales no ocurre. Verificado con `value_counts` sobre los nombres de
    bronze (origen + destino): **2016 y 2024 tienen 0 ocurrencias** de estas
    etiquetas y **2019 solo las trae en los ids 1 y 10** (`ERRONEA` 2.451 y
    `KP1808000368` 19.885, 22.336 en total), sin ningun otro nombre con forma
    de marca de error (busqueda `ERRON` / `KP` + digitos). Los dos ids
    ademas tienen nombre real en 2016.

---

## Verificaciones (resultado de las ejecuciones)

### Conciliacion `bronze = silver + cuarentena` por anio (viajes)

| Anio | bronze | silver | cuarentena | cierra |
|---|---|---|---|---|
| 2016 | 596.807 | 503.252 | 93.555 | si |
| 2019 | 6.217.004 | 6.207.851 | 9.153 | si |
| 2024 | 3.559.284 | 3.555.217 | 4.067 | si |
| **Total** | **10.373.095** | **10.266.320** | **106.775** | **si** |

### Conciliacion de usuarios

| Anio | bronze | silver | cuarentena (dedup) | cierra |
|---|---|---|---|---|
| 2016 | 39.481 | 34.250 | 5.231 | si |
| 2019 | 83.600 | 83.600 | 0 | si |
| 2024 | 197.077 | 197.077 | 0 | si |
| **Total** | **320.158** | **314.927** | **5.231** | **si** |

### Motivos de cuarentena de viajes

| Anio | FECHA_INVALIDA | ESTACION_NULA | FIN_ANTERIOR_A_INICIO | DURACION_MAYOR_24H | DUPLICADO | ID_VIAJE_NULO |
|---|---|---|---|---|---|---|
| 2016 | 93.555 | 0 | 0 | 0 | 0 | 0 |
| 2019 | 0 | 1.760 | 0 | 7.393 | 0 | 0 |
| 2024 | 3.379 | 0 | 0 | 687 | 1 | 0 |

- 2016: 93.555 filas con `fecha_destino_recorrido` vacia.
- 2019: 1.760 filas con `id_estacion_destino` vacio y duraciones de hasta 219
  dias.
- 2024: 3.379 fechas invalidas, 687 duraciones de mas de 24 h y **1 id de
  viaje repetido** (la regla global lo detecto).
- `FIN_ANTERIOR_A_INICIO` e `ID_VIAJE_NULO` no se dispararon con estos datos.

### Otros

- Ejecucion de aceptacion: el pipeline corrio de punta a punta con ``data/``
  conteniendo **solo ``raw/``** (las capas generadas se habian movido), con
  codigo de salida 0 y las mismas cantidades de arriba; los 5 CSV quedaron
  byte a byte identicos a la corrida anterior (mismos SHA-256).

- `id_viaje` unico en `silver/viajes`: **si** (10.266.320 ids distintos).
- `fact_viajes` == `silver/viajes`: **si** (10.266.320 filas).
- Integridad referencial con `dim_fecha` y `dim_estacion`: **si**.
### Viajes sin coincidencia en `dim_usuario` (explicacion obligatoria)

Total: **6.623.887** viajes de `fact_viajes` cuyo `id_usuario` no esta en
`dim_usuario`, sobre 10.266.320 (64,5 %):

| Origen | Viajes |
|---|---|
| 2016: la fuente no trae la columna `id_usuario` | 503.252 |
| 2019: trae `id_usuario` pero no coincide con ningun usuario | 4.410.500 |
| 2024: idem | 1.710.135 |
| **Total** | **6.623.887** |

**Explicacion**: los tres archivos de usuarios son **cortes por anio de
alta**, no el padron completo. Evidencia de los propios datos
(`silver/usuarios`):

| Archivo | `fecha_alta` minima | `fecha_alta` maxima | usuarios |
|---|---|---|---|
| 2016 | 2016-01-01 | 2016-12-31 | 34.250 |
| 2019 | 2019-02-16 | 2019-12-31 | 83.600 |
| 2024 | 2024-01-01 | 2024-12-31 | 197.077 |

- Cada archivo solo contiene altas de su propio anio y **no hay archivo de
  usuarios para 2017, 2018 ni 2020-2023**: todo usuario dado de alta en esos
  anios nunca aparece en `dim_usuario`, asi que sus viajes no pueden cruzarse.
  (Ademas el corte de 2019 arranca recien el 2019-02-16.)
- Los tres archivos **no comparten un solo `id_usuario` entre si** (0 en
  comun): el id no se reutiliza entre cortes, por lo que estar en el corte de
  2016 no garantiza aparecer en el de 2019.
- Por eso el cruce solo puede pegar con los cortes disponibles:
  - **2019**: 1.797.351 de 6.207.851 viajes cruzan (**29 %**); de 420.187 ids
    distintos en los viajes, 91.674 (21,8 %) estan en los cortes de 2016 y
    2019.
  - **2024**: 1.845.082 de 3.555.217 (**51,9 %**); de 262.505 ids distintos,
    164.801 (62,8 %) estan en los tres cortes.

**Impacto en el reporte 3**: la fila `DESCONOCIDO,DESCONOCIDO` suma 6.650.991
viajes = **64,78 %** del total, formada por:
- 6.623.887 viajes sin usuario en `dim_usuario` (desglose de arriba);
- 27.104 viajes de usuarios **si** encontrados pero con `edad` nulada (fuera
  de 16-90) y `genero` `DESCONOCIDO`.

La fila `DESCONOCIDO,<genero>` agrega 438 viajes mas (edad nulada, genero
conocido), con lo que hay **6.651.429** viajes con `rango_etario`
`DESCONOCIDO` en total.
- Top 10 de estaciones por anio: el ranking **cambia entre anios**. 2016 esta
  encabezado por `009 - PARQUE LAS HERAS`, `002 - RETIRO I` y `014 - PACIFICO`,
  y en las posiciones 7 y 8 trae `FACULTAD DE DERECHO` (12.600 viajes) y
  `PUERTO MADERO - UCA` (12.377): son los ids 1 y 10, que en el archivo de
  2019 venian etiquetados `ERRONEA` y `KP1808000368`; esas etiquetas se
  anulan en silver (decision 33) y gold elige el nombre no nulo mas
  reciente. 2019 y 2024 comparten el par `147 - CONSTITUCION` (#1) y
  `014 - PACIFICO` (#2), y ambos anios repiten `009 - PARQUE LAS HERAS`,
  `029 - PARQUE CENTENARIO`, `008 - CONGRESO` y `005 - PLAZA ITALIA` dentro del
  top 10.
- **Limitacion: `id_estacion` no es estable entre anios.** El mismo id puede
  corresponder a otra estacion fisica en otro archivo y, al reves, la misma
  estacion puede aparecer con dos ids: la Facultad de Derecho es el **id 1**
  en 2016 (12.600 viajes) y el **id 202** en 2024 (30.922), y los ids 1 y 10
  no existen en 2024. El prefijo numerico del nombre tampoco coincide siempre
  con el id (id 513 = `308 - SAN MARTIN II`). Solapamiento de ids entre
  archivos: 2016 y 2019 comparten 93, 2016 y 2024 solo 67, y 2019 y 2024 281.
  Se conserva el modelo de la SPEC (una fila por `id_estacion`, con el nombre
  del anio mas reciente) porque ese id es la clave que trae la fuente:
  fusionar estaciones fisicas exigiria cruzar por nombre o coordenadas y
  cambiaria el grain de `fact_viajes`, algo que el enunciado no pide.