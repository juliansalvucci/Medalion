"""Fase 4: modelo estrella en Gold (Tarea 4).

Lee **solo** desde ``data/silver/`` y escribe las dimensiones y el hecho en
``data/gold/``. No contiene logica de limpieza: si hay que corregir algo,
pertenece a silver.

Enfoque de memoria (la maquina tiene poca RAM): ``silver/viajes`` se recorre
**por lotes** en dos pasadas:
  1. recoleccion de datos de las dimensiones (fechas minimas/maximas y
     pares estacion/anio distintos) sin materializar la tabla;
  2. construccion y escritura de ``fact_viajes`` lote por lote, verificando
     integridad referencial en cada lote.

Verificaciones ejecutadas por codigo (falla explicita si no cierran):
  * filas de fact_viajes == filas de silver/viajes
  * todo fecha_key de fact existe en dim_fecha
  * todo id de estacion (origen y destino) existe en dim_estacion
  * conteo (no error) de viajes cuyo id_usuario no esta en dim_usuario
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import GOLD, SILVER

log = logging.getLogger("gold")

LOTE = 250_000

# Mapa fijo de dias de la semana: no depende del locale del sistema.
DIAS_SEMANA = (
    "lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo",
)

# (desde, hasta, etiqueta) para rango_etario
RANGOS_ETARIOS = ((16, 25, "16-25"), (26, 35, "26-35"), (36, 50, "36-50"), (51, 90, "51+"))

_ESQUEMA_FACT = pa.schema([
    pa.field("id_viaje", pa.string()),
    pa.field("fecha_key", pa.int64()),
    pa.field("hora_inicio", pa.int64()),
    pa.field("id_estacion_origen", pa.int64()),
    pa.field("id_estacion_destino", pa.int64()),
    pa.field("id_usuario", pa.string()),
    pa.field("duracion_min", pa.float64()),
    pa.field("es_circular", pa.bool_()),
])

_COLUMNAS_VIAJES = (
    "id_viaje", "fecha_hora_inicio", "fecha_hora_fin", "duracion_seg",
    "id_estacion_origen", "nombre_estacion_origen",
    "id_estacion_destino", "nombre_estacion_destino", "id_usuario", "anio_fuente",
)


class VerificacionError(RuntimeError):
    """Se lanza cuando una verificacion de gold no se cumple."""


# ---------------------------------------------------------------------------
# Lectura de silver por lotes
# ---------------------------------------------------------------------------
def _ruta_silver(nombre: str) -> Path:
    return SILVER / f"{nombre}.parquet"


def _iterar_viajes(columnas: tuple[str, ...] = _COLUMNAS_VIAJES) -> Iterator[pd.DataFrame]:
    """Lotes (row groups) de ``silver/viajes`` en el orden escrito.

    Se pueden pedir solo las columnas necesarias (la pasada 1 de dimensiones
    no necesita todas).
    """
    ruta = _ruta_silver("viajes")
    if not ruta.exists():
        raise VerificacionError(f"Falta {ruta}. Ejecuta primero la capa silver (python main.py).")
    lector = pq.ParquetFile(ruta)
    for lote in lector.iter_batches(columns=list(columnas), batch_size=LOTE):
        yield lote.to_pandas()


# ---------------------------------------------------------------------------
# Dimensiones
# ---------------------------------------------------------------------------
def construir_dim_fecha(desde: pd.Timestamp, hasta: pd.Timestamp) -> pd.DataFrame:
    """Una fila por cada dia continuo entre el primer y el ultimo viaje."""
    dias = pd.date_range(pd.Timestamp(desde).normalize(), pd.Timestamp(hasta).normalize(), freq="D")
    dim = pd.DataFrame({"fecha": dias})
    dim["fecha_key"] = dim["fecha"].dt.strftime("%Y%m%d").astype("int64")
    dim["anio"] = dim["fecha"].dt.year.astype("int64")
    dim["mes"] = dim["fecha"].dt.month.astype("int64")
    dim["dia"] = dim["fecha"].dt.day.astype("int64")
    # weekday(): 0 = lunes ... 6 = domingo, con mapa fijo (sin locale)
    dim["dia_semana"] = pd.Series(dim["fecha"].dt.weekday.to_numpy(), index=dim.index).map(
        {i: nombre for i, nombre in enumerate(DIAS_SEMANA)}
    ).astype("string")
    dim["es_fin_de_semana"] = (dim["fecha"].dt.weekday >= 5).astype(bool)
    return dim[["fecha_key", "fecha", "anio", "mes", "dia", "dia_semana", "es_fin_de_semana"]]


def construir_dim_estacion(pares: set[tuple]) -> pd.DataFrame:
    """Una fila por estacion (origen + destino).

    ``nombre_estacion`` es el nombre normalizado del **anio mas reciente** en
    que aparece (si ese anio no trae nombre, se usa el mas reciente disponible).
    """
    if not pares:
        raise VerificacionError("No se encontraron estaciones en silver/viajes")
    df = pd.DataFrame(sorted(pares, key=lambda t: (t[0], -t[2], str(t[1]))),
                      columns=["id_estacion", "nombre_estacion", "anio_fuente"])
    df["_sin_nombre"] = df["nombre_estacion"].isna()
    df = df.sort_values(["id_estacion", "anio_fuente", "_sin_nombre", "nombre_estacion"],
                        ascending=[True, False, True, True], kind="mergesort")
    dim = (df.groupby("id_estacion", sort=True)
             .agg(nombre_estacion=("nombre_estacion", "first"),
                  primer_anio=("anio_fuente", "min"),
                  ultimo_anio=("anio_fuente", "max"))
             .reset_index())
    dim["id_estacion"] = dim["id_estacion"].astype("int64")
    dim["primer_anio"] = dim["primer_anio"].astype("int64")
    dim["ultimo_anio"] = dim["ultimo_anio"].astype("int64")
    dim["nombre_estacion"] = dim["nombre_estacion"].astype("string")
    return dim[["id_estacion", "nombre_estacion", "primer_anio", "ultimo_anio"]]


def clasificar_edad(edad) -> str:
    """Rango etario; DESCONOCIDO si la edad es nula o esta fuera de 16-90."""
    if pd.isna(edad):
        return "DESCONOCIDO"
    for desde, hasta, etiqueta in RANGOS_ETARIOS:
        if desde <= edad <= hasta:
            return etiqueta
    return "DESCONOCIDO"


def construir_dim_usuario(usuarios: pd.DataFrame) -> pd.DataFrame:
    """dim_usuario desde ``silver/usuarios`` (ya depurado y sin duplicados)."""
    dim = usuarios.copy()
    dim["rango_etario"] = dim["edad"].map(clasificar_edad).astype("string")
    dim["anio_alta"] = dim["fecha_alta"].dt.year.astype("Int64")
    return dim[["id_usuario", "genero", "edad", "rango_etario", "anio_alta"]]


# ---------------------------------------------------------------------------
# Hechos
# ---------------------------------------------------------------------------
def _a_fact(lote: pd.DataFrame) -> pd.DataFrame:
    """Convierte un lote de silver/viajes en filas de fact_viajes."""
    nulos = [c for c in ("id_estacion_origen", "id_estacion_destino") if lote[c].isna().any()]
    if nulos:
        raise VerificacionError(
            f"silver/viajes trae estaciones nulas en {nulos}: la regla de calidad deberia haberlas rechazado"
        )
    inicio = lote["fecha_hora_inicio"]
    if inicio.isna().any():
        raise VerificacionError("silver/viajes trae fecha_hora_inicio nula: revisar reglas de calidad")

    return pd.DataFrame({
        "id_viaje": lote["id_viaje"].astype("string"),
        "fecha_key": inicio.dt.strftime("%Y%m%d").astype("int64"),
        "hora_inicio": inicio.dt.hour.astype("int64"),
        "id_estacion_origen": lote["id_estacion_origen"].astype("int64"),
        "id_estacion_destino": lote["id_estacion_destino"].astype("int64"),
        "id_usuario": lote["id_usuario"].astype("string"),
        "duracion_min": (lote["duracion_seg"].astype("float64") / 60.0).round(1),
        "es_circular": (lote["id_estacion_origen"] == lote["id_estacion_destino"]).astype(bool),
    })


# dtypes de relea de fact (para generar el metadata de pandas del esquema)
_DTYPES_FACT = {"id_viaje": "string", "fecha_key": "int64", "hora_inicio": "int64",
                "id_estacion_origen": "int64", "id_estacion_destino": "int64",
                "id_usuario": "string", "duracion_min": "float64", "es_circular": "bool"}


def _esquema_fact() -> pa.Schema:
    """``_ESQUEMA_FACT`` con metadata de pandas, para que la relea de
    ``fact_viajes`` conserve los mismos dtypes (igual que silver)."""
    referencia = pd.DataFrame({col: pd.Series(dtype=tipo) for col, tipo in _DTYPES_FACT.items()})
    meta = pa.Table.from_pandas(referencia, preserve_index=False).schema.metadata
    return _ESQUEMA_FACT.with_metadata(meta)


# ---------------------------------------------------------------------------
# Verificaciones
# ---------------------------------------------------------------------------
def verificar_gold(fact: pd.DataFrame, dim_fecha: pd.DataFrame, dim_estacion: pd.DataFrame,
                   dim_usuario: pd.DataFrame, acumulados: dict) -> dict:
    """Verifica un lote de fact contra las dimensiones. Acumula estadisticas."""
    claves_fecha = dim_fecha["fecha_key"].to_numpy()
    faltan = ~np.isin(fact["fecha_key"].to_numpy(), claves_fecha)
    if faltan.any():
        raise VerificacionError(
            f"{int(faltan.sum())} viajes con fecha_key ausente en dim_fecha "
            f"(ejemplos: {sorted(set(fact.loc[faltan, 'fecha_key']))[:5]})"
        )

    claves_estacion = dim_estacion["id_estacion"].to_numpy()
    for columna in ("id_estacion_origen", "id_estacion_destino"):
        faltan = ~np.isin(fact[columna].to_numpy(), claves_estacion)
        if faltan.any():
            raise VerificacionError(
                f"{int(faltan.sum())} viajes con {columna} ausente en dim_estacion "
                f"(ejemplos: {sorted(set(fact.loc[faltan, columna]))[:5]})"
            )

    # Usuarios huerfanos: NO es error, solo se cuentan y documentan.
    ids_dim = set(dim_usuario["id_usuario"])
    ids_fact = fact["id_usuario"]
    sin_id = int(ids_fact.isna().sum())
    no_encontrado = int((ids_fact.notna() & ~ids_fact.isin(ids_dim)).sum())
    acumulados["sin_id_usuario"] += sin_id
    acumulados["usuario_no_encontrado"] += no_encontrado


def verificar_conteos(filas_escribidas: int) -> int:
    """filas de fact_viajes == filas de silver/viajes (por metadatos)."""
    esperado = pq.ParquetFile(_ruta_silver("viajes")).metadata.num_rows
    if filas_escribidas != esperado:
        raise VerificacionError(
            f"fact_viajes ({filas_escribidas}) debe tener las mismas filas "
            f"que silver/viajes ({esperado})"
        )
    return esperado


# ---------------------------------------------------------------------------
# Orquestacion
# ---------------------------------------------------------------------------
def ejecutar_gold() -> dict:
    """Construye y guarda el modelo estrella. Devuelve conteos y verificaciones."""
    ruta_viajes = _ruta_silver("viajes")
    if not ruta_viajes.exists():
        raise VerificacionError(f"Falta {ruta_viajes}. Ejecuta primero la capa silver.")

    # --- Pasada 1: datos de las dimensiones (sin materializar la tabla) ---
    minimo: pd.Timestamp | None = None
    maximo: pd.Timestamp | None = None
    pares: set[tuple] = set()
    columnas_p1 = ("fecha_hora_inicio", "id_estacion_origen", "nombre_estacion_origen",
                   "id_estacion_destino", "nombre_estacion_destino", "anio_fuente")
    for lote in _iterar_viajes(columnas_p1):
        fechas = lote["fecha_hora_inicio"].dropna()
        if len(fechas):
            lmin, lmax = fechas.min(), fechas.max()
            minimo = lmin if minimo is None or lmin < minimo else minimo
            maximo = lmax if maximo is None or lmax > maximo else maximo
        origen = lote[["id_estacion_origen", "nombre_estacion_origen", "anio_fuente"]].rename(
            columns={"id_estacion_origen": "id_estacion",
                     "nombre_estacion_origen": "nombre_estacion"})
        destino = lote[["id_estacion_destino", "nombre_estacion_destino", "anio_fuente"]].rename(
            columns={"id_estacion_destino": "id_estacion",
                     "nombre_estacion_destino": "nombre_estacion"})
        for parcial in (origen, destino):
            limpio = parcial.dropna(subset=["id_estacion"])
            pares.update(zip(limpio["id_estacion"].astype("int64"),
                             limpio["nombre_estacion"].where(limpio["nombre_estacion"].notna(), None),
                             limpio["anio_fuente"].astype("int64")))

    if minimo is None or maximo is None:
        raise VerificacionError("silver/viajes esta vacio: no se puede construir gold")

    dim_fecha = construir_dim_fecha(minimo, maximo)
    dim_estacion = construir_dim_estacion(pares)
    log.info("Gold: dim_fecha=%d dias (%s a %s), dim_estacion=%d estaciones",
             len(dim_fecha), dim_fecha['fecha'].min().date(), dim_fecha['fecha'].max().date(),
             len(dim_estacion))

    usuarios = pd.read_parquet(_ruta_silver("usuarios"))
    dim_usuario = construir_dim_usuario(usuarios)
    del usuarios

    # --- Pasada 2: fact_viajes lote por lote, con verificaciones ----------
    GOLD.mkdir(parents=True, exist_ok=True)
    acumulados = {"sin_id_usuario": 0, "usuario_no_encontrado": 0}
    filas = 0
    esquema_fact = _esquema_fact()
    with pq.ParquetWriter(GOLD / "fact_viajes.parquet", esquema_fact) as escritor:
        for lote in _iterar_viajes():
            fact = _a_fact(lote)
            verificar_gold(fact, dim_fecha, dim_estacion, dim_usuario, acumulados)
            escritor.write_table(pa.Table.from_pandas(fact, schema=esquema_fact,
                                                      preserve_index=False))
            filas += len(fact)

    total = verificar_conteos(filas)
    log.info("Gold: fact_viajes=%d filas == silver/viajes=%d OK", filas, total)

    tablas = {"dim_fecha": dim_fecha, "dim_estacion": dim_estacion, "dim_usuario": dim_usuario}
    for nombre, df in tablas.items():
        df.to_parquet(GOLD / f"{nombre}.parquet", index=False)  # reemplazo total
        log.info("Gold: %s -> %d filas", nombre, len(df))

    huerfanos = acumulados["sin_id_usuario"] + acumulados["usuario_no_encontrado"]
    log.info("Gold: viajes sin id_usuario en dim_usuario: %d "
             "(%d sin id, %d con id no encontrado). No es error; ver DECISIONES.md",
             huerfanos, acumulados["sin_id_usuario"], acumulados["usuario_no_encontrado"])

    return {
        "filas_fact": filas,
        "filas_dim_fecha": len(dim_fecha),
        "filas_dim_estacion": len(dim_estacion),
        "filas_dim_usuario": len(dim_usuario),
        "usuarios_huerfanos": huerfanos,
        "usuarios_sin_id": acumulados["sin_id_usuario"],
        "usuarios_no_encontrados": acumulados["usuario_no_encontrado"],
    }