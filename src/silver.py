"""Fase 3: Silver (Tarea 3) — unica capa que conoce las diferencias entre anios.

Lee **solo** desde ``data/bronze/`` y escribe:
  * ``data/silver/viajes.parquet``              (esquema canonico)
  * ``data/silver/viajes_cuarentena.parquet``   (canonico + ``motivo``)
  * ``data/silver/usuarios.parquet``
  * ``data/silver/usuarios_cuarentena.parquet``  (canonico + ``motivo``)

Todo lo que cambia entre anios vive en diccionarios de configuracion
(``MAPEO_VIAJES``, ``MAPEO_USUARIOS``, ``FORMATO_FECHA_*``, ``CONFIG_*``).

Memoria: los viajes se procesan **por anio y por lotes** (row groups de
Parquet), escribiendo cada lote de inmediato; solo los usuarios (320 mil
filas) se cargan completos para poder deduplicarlos globalmente.
"""

from __future__ import annotations

import logging
import unicodedata
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.bronze import contar_filas_bronze
from src.config import ANIOS, BRONZE, COL_ANIO, SILVER

log = logging.getLogger("silver")

# Filas por lote al recorrer bronze (los archivos son enormes)
LOTE = 250_000


class VerificacionError(RuntimeError):
    """Se lanza cuando una verificacion de silver no se cumple."""


# ---------------------------------------------------------------------------
# Configuracion por anio: mapeos de columna (lo que cambia entre anios)
# ---------------------------------------------------------------------------
MAPEO_VIAJES: dict[int, dict[str, str]] = {
    2016: {
        # 2016 no trae id_recorrido ni id_usuario (se sintetiza / se crea nula)
        "fecha_origen_recorrido": "fecha_hora_inicio",
        "fecha_destino_recorrido": "fecha_hora_fin",
        "duracion_recorrido": "duracion_seg",
        "id_estacion_origen": "id_estacion_origen",
        "nombre_estacion_origen": "nombre_estacion_origen",
        "id_estacion_destino": "id_estacion_destino",
        "nombre_estacion_destino": "nombre_estacion_destino",
    },
    2019: {
        "Id_recorrido": "id_viaje",
        "duracion_recorrido": "duracion_seg",
        "fecha_origen_recorrido": "fecha_hora_inicio",
        "fecha_destino_recorrido": "fecha_hora_fin",
        "id_estacion_origen": "id_estacion_origen",
        "nombre_estacion_origen": "nombre_estacion_origen",
        "id_estacion_destino": "id_estacion_destino",
        "nombre_estacion_destino": "nombre_estacion_destino",
        "id_usuario": "id_usuario",
        # "Unnamed: 0" es un indice residual del CSV: no se mapea, se descarta
    },
    2024: {
        "id_recorrido": "id_viaje",
        "duracion_recorrido": "duracion_seg",
        "fecha_origen_recorrido": "fecha_hora_inicio",
        "fecha_destino_recorrido": "fecha_hora_fin",
        "id_estacion_origen": "id_estacion_origen",
        "nombre_estacion_origen": "nombre_estacion_origen",
        "id_estacion_destino": "id_estacion_destino",
        "nombre_estacion_destino": "nombre_estacion_destino",
        "id_usuario": "id_usuario",
    },
}

MAPEO_USUARIOS: dict[int, dict[str, str]] = {
    2016: {
        "id_usuario": "id_usuario",
        "genero_usuario": "genero",
        "edad_usuario": "edad",
        "fecha_alta": "fecha_alta",
        "hora_alta": "hora_alta",
    },
    2019: {
        "ID_usuario": "id_usuario",
        "genero_usuario": "genero",
        "edad_usuario": "edad",
        "fecha_alta": "fecha_alta",
        "hora_alta": "hora_alta",
    },
    2024: {
        "id_usuario": "id_usuario",
        "genero_usuario": "genero",
        "edad_usuario": "edad",
        "fecha_alta": "fecha_alta",
        "hora_alta": "hora_alta",
    },
}

# Formato de fecha y hora por anio (detectado en la Fase 1)
FORMATO_FECHA_VIAJES: dict[int, str] = {
    2016: "%Y-%m-%d %H:%M:%S",
    2019: "%Y-%m-%d %H:%M:%S",
    2024: "%Y-%m-%d %H:%M:%S",
}

# (formato de fecha, formato de hora) para fecha_alta de usuarios
FORMATO_FECHA_USUARIOS: dict[int, tuple[str, str]] = {
    2016: ("%d-%m-%y", "%I:%M:%S %p"),
    2019: ("%Y-%m-%d", "%H:%M:%S"),
    2024: ("%Y-%m-%d", "%H:%M:%S"),
}

# Unidad/representacion de la duracion por anio
#   timedelta_texto         -> "0 days 00:16:29.000000000"
#   segundos_separador_miles-> segundos con "." o "," de miles ("1.185" = 1185)
#   segundos                -> entero simple ("568")
CONFIG_DURACION: dict[int, str] = {
    2016: "timedelta_texto",
    2019: "segundos_separador_miles",
    2024: "segundos",
}

# Raros de formato en los id por anio (ver DECISIONES.md)
CONFIG_IDS: dict[int, dict] = {
    # En 2019 TODOS los id vienen con el sufijo "BAEcobici" adjunto
    2019: {"sufijo": "BAEcobici"},
    # En 2024 el id_usuario viene en formato float ("992557.0")
    2024: {"usuario_float": True},
}

# Mapeo real de valores de genero (ver DECISIONES.md por los valores observados)
MAPEO_GENERO: dict[str, str] = {
    "": "DESCONOCIDO",
    "F": "F", "FEMALE": "F", "FEMENINO": "F",
    "M": "M", "MALE": "M", "MASCULINO": "M",
    "OTHER": "OTRO", "OTRO": "OTRO", "X": "OTRO",
}

# Edades admitidas: fuera de rango -> nulo (no se rechaza el usuario)
EDAD_MIN, EDAD_MAX = 16, 90

# ---------------------------------------------------------------------------
# Esquemas canonicos (orden y tipo exactos)
# ---------------------------------------------------------------------------
ESQUEMA_VIAJES: dict[str, str] = {
    "id_viaje": "string",
    "fecha_hora_inicio": "datetime64[ns]",
    "fecha_hora_fin": "datetime64[ns]",
    "duracion_seg": "Int64",
    "id_estacion_origen": "Int64",
    "nombre_estacion_origen": "string",
    "id_estacion_destino": "Int64",
    "nombre_estacion_destino": "string",
    "id_usuario": "string",
    "anio_fuente": "Int64",
}

ESQUEMA_USUARIOS: dict[str, str] = {
    "id_usuario": "string",
    "genero": "string",
    "edad": "Int64",
    "fecha_alta": "datetime64[ns]",
    "anio_fuente": "Int64",
}

ESQUEMA_VIAJES_CUARENTENA: dict[str, str] = {**ESQUEMA_VIAJES, "motivo": "string"}
ESQUEMA_USUARIOS_CUARENTENA: dict[str, str] = {**ESQUEMA_USUARIOS, "motivo": "string"}

_TIPO_ARROW = {"string": pa.string(), "Int64": pa.int64(), "datetime64[ns]": pa.timestamp("ns")}


def _arrow_schema(esquema: dict[str, str]) -> pa.Schema:
    return pa.schema([pa.field(col, _TIPO_ARROW[tipo]) for col, tipo in esquema.items()])


def _esquema_con_metadata(esquema: dict[str, str]) -> pa.Schema:
    """Esquema Arrow **con** el metadata de pandas.

    ``ParquetWriter`` escribe el esquema que recibe en el constructor; si no
    trae metadata de pandas, ``pd.read_parquet`` devuelve ``int64``/``str``
    en vez de ``Int64``/``string`` (los dtypes del enunciado) en columnas sin
    nulos. Se genera el metadata a partir de un frame de referencia vacio con
    los mismos dtypes, de modo que la relea sea identica al esquema validado
    antes de guardar.
    """
    referencia = pd.DataFrame({col: pd.Series(dtype=tipo) for col, tipo in esquema.items()})
    meta = pa.Table.from_pandas(referencia, preserve_index=False).schema.metadata
    return _arrow_schema(esquema).with_metadata(meta)


def validar_esquema(df: pd.DataFrame, esquema: dict[str, str]) -> None:
    """Compara dtypes y orden de columnas contra el esquema esperado.

    Lanza :class:`VerificacionError` si algo no coincide. Se llama siempre
    antes de guardar.
    """
    esperado = list(esquema)
    if list(df.columns) != esperado:
        raise VerificacionError(
            f"Orden de columnas incorrecto. Obtenido: {list(df.columns)} | Esperado: {esperado}"
        )
    for col, tipo in esquema.items():
        actual = str(df[col].dtype)
        if actual != tipo:
            raise VerificacionError(
                f"Columna '{col}': dtype {actual!r} distinto del esperado {tipo!r}"
            )
# ---------------------------------------------------------------------------
# Transformacion: de un lote de bronze al esquema canonico
# ---------------------------------------------------------------------------
def renombrar(df: pd.DataFrame, anio: int, mapeo: dict[str, str] | None = None) -> pd.DataFrame:
    """Renombra columnas segun el mapeo del anio y descarta las que no estan.

    ``_anio`` (metadato de bronze) se conserva siempre: alimenta a
    ``anio_fuente``.
    """
    if mapeo is None:
        mapeo = MAPEO_VIAJES[anio]
    presentes = {orig: canon for orig, canon in mapeo.items() if orig in df.columns}
    ausentes = [orig for orig in mapeo if orig not in df.columns]
    if ausentes:
        log.info("Anio %d: columnas del mapeo ausentes en bronze: %s", anio, ausentes)
    out = df[list(presentes)].rename(columns=presentes)
    if COL_ANIO in df.columns:
        out[COL_ANIO] = df[COL_ANIO]
    return out


def normalizar_id(serie: pd.Series, anio: int) -> pd.Series:
    """Limpia los id segun la configuracion del anio y recorta espacios.

    * 2019: todos los id traen el sufijo ``BAEcobici`` adjunto.
    * 2024: ``id_usuario`` viene en formato float (``992557.0``).
    """
    cfg = CONFIG_IDS.get(anio, {})
    if cfg.get("sufijo"):
        serie = serie.str.replace(cfg["sufijo"], "", regex=False)
    if cfg.get("usuario_float"):
        serie = serie.str.replace(r"\.0$", "", regex=True)
    return serie.str.strip()


def normalizar_nombre(serie: pd.Series) -> pd.Series:
    """strip + mayusculas + espacios multiples -> uno solo. Nulos quedan nulos."""
    limpio = serie.astype("string").str.strip().str.replace(r"\s+", " ", regex=True).str.upper()
    return limpio.replace("", pd.NA)


def _sin_tildes(texto: str) -> str:
    descompuesto = unicodedata.normalize("NFD", texto)
    return "".join(c for c in descompuesto if unicodedata.category(c) != "Mn")


def normalizar_genero(serie: pd.Series) -> pd.Series:
    """Valores reales -> F / M / OTRO / DESCONOCIDO.

    Comparacion normalizada: strip + mayusculas + sin tildes. Cualquier valor
    no reconocido (incluido vacio) -> DESCONOCIDO.
    """
    base = serie.astype("string").fillna("").str.strip().str.upper()
    base = base.map(_sin_tildes)
    mapeado = base.map(MAPEO_GENERO)
    return mapeado.astype("string").fillna("DESCONOCIDO")


def _quitar_separadores_miles(texto: pd.Series) -> pd.Series:
    """``"1.185"`` / ``"2,814"`` -> ``1185`` / ``2814`` (solo si es miles)."""
    return texto.str.replace(r"(?<=\d)[.,](?=\d{3}(?!\d))", "", regex=True)


def calcular_duracion(df: pd.DataFrame, anio: int) -> pd.Series:
    """Duracion en segundos (``Int64``).

    Si el anio no trae duracion, o no se pudo parsear, se calcula como
    ``fecha_hora_fin - fecha_hora_inicio``.
    """
    modo = CONFIG_DURACION.get(anio, "desde_fechas")
    seg: pd.Series | None = None
    if "duracion_seg" in df.columns:
        serie = df["duracion_seg"]
        if modo == "timedelta_texto":
            seg = pd.to_timedelta(serie, errors="coerce").dt.total_seconds()
        elif modo == "segundos_separador_miles":
            limpio = _quitar_separadores_miles(serie.astype("string").fillna(""))
            seg = pd.to_numeric(limpio, errors="coerce")
        elif modo == "segundos":
            seg = pd.to_numeric(serie, errors="coerce")
    if seg is None:
        seg = pd.Series(np.nan, index=df.index, dtype="float64")

    calculada = (df["fecha_hora_fin"] - df["fecha_hora_inicio"]).dt.total_seconds()
    return seg.fillna(calculada).round().astype("Int64")


def _columnas_requeridas(df: pd.DataFrame, anio: int, requeridas: tuple[str, ...]) -> None:
    faltantes = [c for c in requeridas if c not in df.columns]
    if faltantes:
        raise VerificacionError(
            f"Bronze {anio}: faltan columnas requeridas {faltantes} (revisa el mapeo del anio)"
        )


def transformar_viajes(df: pd.DataFrame, anio: int, offset: int = 0) -> pd.DataFrame:
    """De un lote de bronze al esquema canonico de ``silver/viajes``."""
    df = renombrar(df, anio, MAPEO_VIAJES[anio])
    _columnas_requeridas(
        df, anio,
        ("fecha_hora_inicio", "fecha_hora_fin", "id_estacion_origen",
         "id_estacion_destino", "nombre_estacion_origen", "nombre_estacion_destino"),
    )

    formato = FORMATO_FECHA_VIAJES[anio]
    df["fecha_hora_inicio"] = pd.to_datetime(
        df["fecha_hora_inicio"], format=formato, errors="coerce").astype("datetime64[ns]")
    df["fecha_hora_fin"] = pd.to_datetime(
        df["fecha_hora_fin"], format=formato, errors="coerce").astype("datetime64[ns]")

    if "id_viaje" in df.columns:
        df["id_viaje"] = normalizar_id(df["id_viaje"], anio).astype("string")
    else:
        # El anio no trae id de viaje en la fuente: se sintetiza un id
        # estable por posicion de fila (deterministico entre ejecuciones).
        # Documentado en DECISIONES.md.
        df["id_viaje"] = pd.Series(
            [f"{anio}-{offset + i}" for i in range(len(df))], index=df.index, dtype="string"
        )

    for col in ("id_estacion_origen", "id_estacion_destino"):
        if col in df.columns:
            df[col] = pd.to_numeric(normalizar_id(df[col], anio), errors="coerce").astype("Int64")
        else:
            df[col] = pd.Series(pd.NA, index=df.index, dtype="Int64")

    if "id_usuario" in df.columns:
        df["id_usuario"] = normalizar_id(df["id_usuario"], anio).astype("string").replace("", pd.NA)
    else:
        # Sin id de usuario en el anio: columna nula
        df["id_usuario"] = pd.Series(pd.NA, index=df.index, dtype="string")

    df["duracion_seg"] = calcular_duracion(df, anio)
    df["anio_fuente"] = pd.to_numeric(df[COL_ANIO], errors="coerce").astype("Int64")

    for col in ("nombre_estacion_origen", "nombre_estacion_destino"):
        df[col] = normalizar_nombre(df[col]) if col in df.columns else pd.Series(
            pd.NA, index=df.index, dtype="string")

    return df[list(ESQUEMA_VIAJES)]


def _parsear_fecha_alta(df: pd.DataFrame, anio: int) -> pd.Series:
    """fecha_alta combinando fecha + hora, con respaldo a solo la fecha."""
    fmt_fecha, fmt_hora = FORMATO_FECHA_USUARIOS[anio]
    fecha = df["fecha_alta"].astype("string").fillna("").str.strip()
    if "hora_alta" in df.columns:
        hora = df["hora_alta"].astype("string").fillna("").str.strip()
    else:
        hora = pd.Series("", index=df.index, dtype="string")
    completo = pd.to_datetime(fecha + " " + hora, format=f"{fmt_fecha} {fmt_hora}", errors="coerce")
    solo_fecha = pd.to_datetime(fecha, format=fmt_fecha, errors="coerce")
    return completo.fillna(solo_fecha).astype("datetime64[ns]")


def transformar_usuarios(df: pd.DataFrame, anio: int) -> pd.DataFrame:
    """De un archivo de bronze al esquema canonico de ``silver/usuarios``."""
    df = renombrar(df, anio, MAPEO_USUARIOS[anio])
    _columnas_requeridas(df, anio, ("id_usuario", "genero", "edad", "fecha_alta"))

    df["id_usuario"] = normalizar_id(df["id_usuario"], anio).astype("string").replace("", pd.NA)
    df["genero"] = normalizar_genero(df["genero"])

    edad = pd.to_numeric(df["edad"], errors="coerce")
    # Edad fuera de 16-90 -> nulo (no se rechaza el usuario). Ver DECISIONES.md
    df["edad"] = edad.where(edad.between(EDAD_MIN, EDAD_MAX)).astype("Int64")

    df["fecha_alta"] = _parsear_fecha_alta(df, anio)
    df["anio_fuente"] = pd.to_numeric(df[COL_ANIO], errors="coerce").astype("Int64")
    return df[list(ESQUEMA_USUARIOS)]


# ---------------------------------------------------------------------------
# Calidad de viajes: nada se borra, todo va a cuarentena con su motivo
# ---------------------------------------------------------------------------
def _bool(serie: pd.Series) -> pd.Series:
    """Condiciones sin NA: un NA en la condicion cuenta como False."""
    return serie.fillna(False).astype(bool)


class ControlDuplicados:
    """Ids de viaje vistos entre lotes y entre anios (regla DUPLICADO global).

    Se usa un set de ids acumulado entre anios, tal como recomienda la SPEC
    cuando el volumen no permite cargar todo en memoria.
    """

    def __init__(self) -> None:
        self._vistos: set[str] = set()

    def contiene(self, ids) -> np.ndarray:
        vistos = self._vistos
        return np.fromiter((i in vistos for i in ids), dtype=bool, count=len(ids))

    def agregar(self, ids) -> None:
        self._vistos.update(ids)

    def __len__(self) -> int:
        return len(self._vistos)


def aplicar_calidad(df: pd.DataFrame,
                    control: ControlDuplicados | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Aplica las reglas de calidad a los viajes.

    Un registro que falla **no se borra**: pasa a ``viajes_cuarentena`` con la
    columna ``motivo``. Si falla varias reglas se guarda solo el primer motivo
    en este orden: FECHA_INVALIDA, ESTACION_NULA, FIN_ANTERIOR_A_INICIO,
    DURACION_MAYOR_24H, DUPLICADO, ID_VIAJE_NULO.
    """
    motivo = pd.Series(pd.NA, index=df.index, dtype="string")

    # 1. fechas nulas
    sin_fechas = df["fecha_hora_inicio"].isna() | df["fecha_hora_fin"].isna()
    motivo = motivo.mask(_bool(sin_fechas), "FECHA_INVALIDA")

    # 2. estacion de origen o destino nula
    sin_estacion = df["id_estacion_origen"].isna() | df["id_estacion_destino"].isna()
    motivo = motivo.mask(_bool(motivo.isna() & sin_estacion), "ESTACION_NULA")

    # 3. fin anterior al inicio
    fin_malo = df["fecha_hora_fin"] < df["fecha_hora_inicio"]
    motivo = motivo.mask(_bool(motivo.isna() & fin_malo), "FIN_ANTERIOR_A_INICIO")

    # 4. duracion mayor a 24 horas
    larga = df["duracion_seg"] > 86_400
    motivo = motivo.mask(_bool(motivo.isna() & larga), "DURACION_MAYOR_24H")

    # 5. id_viaje repetido: global entre anios (control) y dentro del lote.
    #    Solo se evalua sobre filas que aun no tienen motivo y con id no nulo.
    candidatos = motivo.isna() & df["id_viaje"].notna() & (df["id_viaje"].str.len().fillna(0) > 0)
    if control is not None and bool(candidatos.any()):
        ids = df.loc[candidatos, "id_viaje"]
        es_dup = pd.Series(False, index=df.index, dtype=bool)
        es_dup.loc[ids.index] = ids.duplicated(keep="first") | pd.Series(
            control.contiene(ids), index=ids.index, dtype=bool)
        motivo = motivo.mask(_bool(motivo.isna() & es_dup), "DUPLICADO")
        control.agregar(ids)

    # 6. id_viaje nulo o vacio (motivo extra al final, DECISIONES.md)
    sin_id = df["id_viaje"].isna() | (df["id_viaje"].str.len().fillna(0) == 0)
    motivo = motivo.mask(_bool(motivo.isna() & sin_id), "ID_VIAJE_NULO")

    validos = df[motivo.isna()].copy()
    cuarentena = df[motivo.notna()].copy()
    cuarentena["motivo"] = motivo[motivo.notna()].astype("string")
    return validos, cuarentena


def aplicar_calidad_usuarios(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Regla minima de usuarios: ``id_usuario`` no nulo.

    Los rechazados van a ``usuarios_cuarentena`` con motivo
    ``ID_USUARIO_NULO`` (decision propia, ver DECISIONES.md).
    """
    motivo = pd.Series(pd.NA, index=df.index, dtype="string")
    sin_id = df["id_usuario"].isna() | (df["id_usuario"].str.len().fillna(0) == 0)
    motivo = motivo.mask(_bool(sin_id), "ID_USUARIO_NULO")

    validos = df[motivo.isna()].copy()
    cuarentena = df[motivo.notna()].copy()
    cuarentena["motivo"] = motivo[motivo.notna()].astype("string")
    return validos, cuarentena


def deduplicar_usuarios(validos: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Dedup **global** por ``id_usuario``: gana la ``fecha_alta`` mas reciente.

    Empates (o fechas NaT) se resuelven de forma estable y documentada:
    1) gana el ``anio_fuente`` mas reciente; 2) si sigue empando, se conserva
    la primera aparicion en el orden de lectura (2016 -> 2019 -> 2024).
    Los descartados van a cuarentena con motivo ``DUPLICADO_USUARIO``.
    """
    orden = validos.copy()
    # NaT se trata como la fecha mas antigua: pierde frente a cualquier fecha real
    orden["_fecha_clave"] = orden["fecha_alta"].fillna(pd.Timestamp("1900-01-01"))
    orden = orden.sort_values(["id_usuario", "_fecha_clave", "anio_fuente"],
                              ascending=[True, False, False], kind="mergesort")
    es_dup = orden["id_usuario"].duplicated(keep="first")

    quedan = orden.loc[~es_dup].drop(columns=["_fecha_clave"]).reset_index(drop=True)
    salen = orden.loc[es_dup].drop(columns=["_fecha_clave"]).reset_index(drop=True)
    salen["motivo"] = pd.Series("DUPLICADO_USUARIO", index=salen.index, dtype="string")
    return quedan, salen
# ---------------------------------------------------------------------------
# Lectura de bronze y escritura de silver
# ---------------------------------------------------------------------------
def _columnas_bronze(tipo: str, anio: int) -> list[str]:
    """Columnas originales que silver necesita de bronze (+ metadato ``_anio``)."""
    mapeo = MAPEO_VIAJES[anio] if tipo == "recorridos" else MAPEO_USUARIOS[anio]
    return list(mapeo) + [COL_ANIO]


def _ruta_bronze(tipo: str, anio: int) -> Path:
    return BRONZE / tipo / f"anio={anio}" / "data.parquet"


def _iterar_bronze(tipo: str, anio: int) -> Iterator[tuple[int, pd.DataFrame]]:
    """Lotes (row groups) de bronze con el offset de fila del archivo origen.

    El offset permite sintetizar ids estables sin cargar el anio completo.
    """
    ruta = _ruta_bronze(tipo, anio)
    if not ruta.exists():
        raise VerificacionError(f"Falta {ruta}. Ejecuta primero la capa bronze (python main.py).")
    lector = pq.ParquetFile(ruta)
    disponibles = set(lector.schema_arrow.names)
    columnas = [c for c in _columnas_bronze(tipo, anio) if c in disponibles]
    offset = 0
    for lote in lector.iter_batches(columns=columnas, batch_size=LOTE):
        df = lote.to_pandas()
        yield offset, df
        offset += len(df)


def _leer_bronze_usuarios(anio: int) -> pd.DataFrame:
    """Los usuarios son pocos (max 197 mil filas): se cargan completos."""
    ruta = _ruta_bronze("usuarios", anio)
    if not ruta.exists():
        raise VerificacionError(f"Falta {ruta}. Ejecuta primero la capa bronze (python main.py).")
    lector = pq.ParquetFile(ruta)
    disponibles = set(lector.schema_arrow.names)
    columnas = [c for c in _columnas_bronze("usuarios", anio) if c in disponibles]
    return lector.read(columns=columnas).to_pandas()


def _procesar_usuarios() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Procesa los 3 anios, deduplica globalmente y devuelve (silver, cuarentena)."""
    marcos_validos: list[pd.DataFrame] = []
    marcos_cuar: list[pd.DataFrame] = []
    for anio in ANIOS:
        df = transformar_usuarios(_leer_bronze_usuarios(anio), anio)
        validos, cuarentena = aplicar_calidad_usuarios(df)
        log.info("Silver usuarios %d: %d validos, %d a cuarentena", anio, len(validos), len(cuarentena))
        marcos_validos.append(validos)
        marcos_cuar.append(cuarentena)

    todos = pd.concat(marcos_validos, ignore_index=True)
    nulos = pd.concat(marcos_cuar, ignore_index=True)
    quedan, salen = deduplicar_usuarios(todos)
    if len(salen):
        log.info("Silver usuarios: %d descartados por dedup global (DUPLICADO_USUARIO)", len(salen))
    cuarentena = pd.concat([nulos, salen], ignore_index=True)
    return quedan, cuarentena


# ---------------------------------------------------------------------------
# Verificaciones y conciliacion
# ---------------------------------------------------------------------------
def _conteos_anio(ruta: Path, columna: str = "anio_fuente") -> dict[int, int]:
    """Conteo por anio leyendo **solo** una columna del Parquet."""
    serie = pd.read_parquet(ruta, columns=[columna])[columna]
    return {int(k): int(v) for k, v in serie.value_counts().items()}


def _conciliar(bronze: dict[int, int], silver: dict[int, int], cuarentena: dict[int, int],
               etiqueta: str) -> None:
    """``filas_bronze == filas_silver + filas_cuarentena`` por anio."""
    for anio in ANIOS:
        b, s, c = bronze.get(anio, 0), silver.get(anio, 0), cuarentena.get(anio, 0)
        if b != s + c:
            raise VerificacionError(
                f"Conciliacion {etiqueta} {anio}: bronze ({b}) != silver ({s}) + cuarentena ({c})"
            )
        log.info("Conciliacion %s %d OK: %d = %d + %d", etiqueta, anio, b, s, c)


def _verificar_unicidad_viajes() -> int:
    """``id_viaje`` unico en ``silver/viajes`` (leyendo solo esa columna)."""
    ids = pd.read_parquet(SILVER / "viajes.parquet", columns=["id_viaje"])["id_viaje"]
    repetidos = int(ids.duplicated().sum())
    if repetidos:
        raise VerificacionError(
            f"id_viaje no es unico en silver/viajes: {repetidos} filas repetidas"
        )
    total = len(ids)
    del ids
    return total


# ---------------------------------------------------------------------------
# Orquestacion
# ---------------------------------------------------------------------------
def ejecutar_silver() -> dict:
    """Ejecuta la capa silver completa y verifica. Devuelve conteos."""
    SILVER.mkdir(parents=True, exist_ok=True)
    control = ControlDuplicados()

    esquema_v = _esquema_con_metadata(ESQUEMA_VIAJES)
    esquema_vc = _esquema_con_metadata(ESQUEMA_VIAJES_CUARENTENA)

    with pq.ParquetWriter(SILVER / "viajes.parquet", esquema_v) as esc_v, \
            pq.ParquetWriter(SILVER / "viajes_cuarentena.parquet", esquema_vc) as esc_vc:
        for anio in ANIOS:
            filas_ok = filas_cuar = 0
            for offset, lote in _iterar_bronze("recorridos", anio):
                df = transformar_viajes(lote, anio, offset)
                validos, cuarentena = aplicar_calidad(df, control)
                validar_esquema(validos, ESQUEMA_VIAJES)
                validar_esquema(cuarentena, ESQUEMA_VIAJES_CUARENTENA)
                esc_v.write_table(pa.Table.from_pandas(validos, schema=esquema_v,
                                                        preserve_index=False))
                esc_vc.write_table(pa.Table.from_pandas(cuarentena, schema=esquema_vc,
                                                         preserve_index=False))
                filas_ok += len(validos)
                filas_cuar += len(cuarentena)
            log.info("Silver viajes %d: %d validos, %d a cuarentena", anio, filas_ok, filas_cuar)

    ids_controlados = len(control)
    del control  # libera memoria antes de las verificaciones

    usuarios, usuarios_cuar = _procesar_usuarios()
    validar_esquema(usuarios, ESQUEMA_USUARIOS)
    validar_esquema(usuarios_cuar, ESQUEMA_USUARIOS_CUARENTENA)
    usuarios.to_parquet(SILVER / "usuarios.parquet", index=False)          # reemplazo total
    usuarios_cuar.to_parquet(SILVER / "usuarios_cuarentena.parquet", index=False)

    # --- Verificaciones sobre lo efectivamente escrito -------------------
    total_viajes = _verificar_unicidad_viajes()

    _conciliar(
        contar_filas_bronze("recorridos"),
        _conteos_anio(SILVER / "viajes.parquet"),
        _conteos_anio(SILVER / "viajes_cuarentena.parquet"),
        "viajes",
    )
    _conciliar(
        contar_filas_bronze("usuarios"),
        _conteos_anio(SILVER / "usuarios.parquet"),
        _conteos_anio(SILVER / "usuarios_cuarentena.parquet"),
        "usuarios",
    )

    return {
        "viajes": {
            "silver": _conteos_anio(SILVER / "viajes.parquet"),
            "cuarentena": _conteos_anio(SILVER / "viajes_cuarentena.parquet"),
            "total": total_viajes,
        },
        "usuarios": {
            "silver": _conteos_anio(SILVER / "usuarios.parquet"),
            "cuarentena": _conteos_anio(SILVER / "usuarios_cuarentena.parquet"),
        },
        "ids_viaje_vistos": ids_controlados,
    }