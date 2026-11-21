"""Fase 2: Bronze (Tarea 2) — copia fiel de data/raw/, sin limpieza.

Requisitos cumplidos:
  * ``dtype=str`` + ``keep_default_na=False``: todo queda como texto.
  * Lectura por ``chunksize`` (los archivos son enormes).
  * Escritura con ``pyarrow.parquet.ParquetWriter`` en modo ``w`` (reemplaza
    la salida anterior; nunca se hace append).
  * Tres columnas de metadatos: ``_archivo_origen``, ``_fecha_ingesta`` (un
    solo valor por ejecucion) y ``_anio``.
  * ``data/raw/`` es de solo lectura: nunca se modifica.
"""

from __future__ import annotations

import logging
import zipfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import (
    ANIOS,
    BRONZE,
    CHUNKSIZE,
    COL_ANIO,
    COL_ARCHIVO_ORIGEN,
    COL_FECHA_INGESTA,
    RAW,
    TIPOS,
    detectar_archivos,
)

log = logging.getLogger("bronze")

# ---------------------------------------------------------------------------
# Configuracion de lectura por anio (separador, encoding y archivo interno del
# ZIP). Los valores fueron **detectados** en la Fase 1 (ver DECISIONES.md).
# Si falta alguno, se vuelve a detectar automaticamente.
# ---------------------------------------------------------------------------
CONFIG_LECTURA: dict[int, dict[str, dict[str, str]]] = {
    2016: {
        "recorridos": {"sep": ",", "encoding": "utf-8-sig", "interno": "recorridos-realizados-2016.csv"},
        "usuarios": {"sep": ",", "encoding": "utf-8-sig"},
    },
    2019: {
        "recorridos": {"sep": ",", "encoding": "utf-8-sig", "interno": "trips_2019.csv"},
        "usuarios": {"sep": ",", "encoding": "utf-8-sig"},
    },
    2024: {
        "recorridos": {"sep": ",", "encoding": "utf-8-sig",
                       "interno": "badata_ecobici_recorridos_realizados_2024.csv"},
        "usuarios": {"sep": ",", "encoding": "utf-8-sig"},
    },
}


def detectar_tipo(ruta: Path) -> str:
    """'usuarios' o 'recorridos' segun el nombre del archivo."""
    return "usuarios" if "usuario" in ruta.name.lower() else "recorridos"


def elegir_csv_interno(nombres: list[str]) -> str:
    """Elige el CSV correcto si el ZIP trae mas de un archivo."""
    csvs = [n for n in nombres if n.lower().endswith(".csv")]
    if not csvs:
        raise ValueError(f"El ZIP no contiene ningun CSV: {nombres}")
    preferidos = [n for n in csvs if any(k in n.lower() for k in ("recorrido", "viaje", "trip"))]
    return (preferidos or csvs)[0]


@contextmanager
def _abrir_fuente(ruta: Path, cfg: dict) -> Iterator:
    """Abre el CSV de origen: directamente o como CSV interno de un ZIP.

    El manejador es binario; pandas decodifica con ``cfg['encoding']``.
    """
    if ruta.suffix.lower() == ".zip":
        with zipfile.ZipFile(ruta) as zf:
            interno = cfg.get("interno") or elegir_csv_interno(zf.namelist())
            with zf.open(interno) as crudo:
                yield crudo
    else:
        with open(ruta, "rb") as crudo:
            yield crudo


def _config_lectura(ruta: Path, anio: int) -> dict:
    """Configuracion de lectura para (anio, tipo), con deteccion de respaldo."""
    tipo = detectar_tipo(ruta)
    cfg = dict(CONFIG_LECTURA.get(anio, {}).get(tipo, {}))

    if "sep" not in cfg or "encoding" not in cfg:
        with _abrir_fuente(ruta, {"interno": cfg.get("interno")}) as fh:
            muestra = fh.read(65_536)
        encoding = cfg.get("encoding") or _detectar_encoding(muestra)
        sep = cfg.get("sep") or _detectar_separador(muestra.decode(encoding, errors="replace"))
        cfg.update({"encoding": encoding, "sep": sep})
    return cfg


def _detectar_encoding(muestra: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8"):
        try:
            muestra.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _detectar_separador(texto: str) -> str:
    from collections import Counter
    lineas = [l for l in texto.splitlines() if l.strip()][:50]
    mejor, puntaje_mejor = ",", -1.0
    for sep in (",", ";", "\t", "|"):
        if not lineas:
            break
        conteos = [l.count(sep) for l in lineas]
        if max(conteos) == 0:
            continue
        moda = Counter(conteos).most_common(1)[0]
        puntaje = moda[1] / len(lineas) * moda[0]
        if puntaje > puntaje_mejor:
            mejor, puntaje_mejor = sep, puntaje
    return mejor


def _a_tabla_todo_string(chunk: pd.DataFrame) -> pa.Table:
    """Convierte un chunk a una tabla Arrow con **todos** los campos ``string``."""
    valores = {}
    for col in chunk.columns:
        serie = chunk[col]
        try:
            valores[col] = pa.array(serie, type=pa.string())
        except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError):
            # Respaldo: pasar por object para conservar ``None`` como nulo
            valores[col] = pa.array(serie.astype(object).where(serie.notna(), None), type=pa.string())
    return pa.Table.from_pydict(valores)


def ingestar_archivo(ruta: Path, anio: int, destino: Path, fecha_ingesta: str | None = None) -> int:
    """Lee un archivo crudo en chunks y escribe un unico ``data.parquet``.

    Copia fiel: sin renombrar, sin convertir tipos, sin eliminar filas. Solo
    se agregan las tres columnas de metadatos. El archivo de destino se
    **reemplaza** (ParquetWriter en modo ``w``).

    Devuelve la cantidad de filas escritas.
    """
    tipo = detectar_tipo(ruta)
    cfg = _config_lectura(ruta, anio)
    if fecha_ingesta is None:
        fecha_ingesta = datetime.now().isoformat(timespec="seconds")

    destino.parent.mkdir(parents=True, exist_ok=True)
    escritas = 0
    writer: pq.ParquetWriter | None = None

    with _abrir_fuente(ruta, cfg) as fh:
        chunks = pd.read_csv(
            fh,
            sep=cfg["sep"],
            encoding=cfg["encoding"],
            dtype=str,
            keep_default_na=False,
            chunksize=CHUNKSIZE,
        )
        for chunk in chunks:
            chunk = chunk.copy()
            chunk[COL_ARCHIVO_ORIGEN] = ruta.name
            chunk[COL_FECHA_INGESTA] = fecha_ingesta
            chunk[COL_ANIO] = str(anio)
            tabla = _a_tabla_todo_string(chunk)
            if writer is None:
                writer = pq.ParquetWriter(destino, tabla.schema)  # modo "w": reemplaza
            writer.write_table(tabla)
            escritas += len(chunk)

    if writer is None:
        raise RuntimeError(f"No se pudo leer ninguna fila de {ruta} (¿archivo vacío?)")
    writer.close()

    log.info("Bronze %s %d: %s -> %s (%d filas)", tipo, anio, ruta.name, destino, escritas)
    return escritas


def contar_filas_bronze(tipo: str = "recorridos") -> dict[int, int]:
    """Conteo por anio leyendo **solo** los metadatos del Parquet (sin datos)."""
    conteos: dict[int, int] = {}
    base = BRONZE / tipo
    if not base.exists():
        return conteos
    for carpeta in sorted(base.glob("anio=*")):
        anio = int(carpeta.name.split("=")[1])
        ruta = carpeta / "data.parquet"
        if ruta.exists():
            conteos[anio] = pq.ParquetFile(ruta).metadata.num_rows
    return conteos


def ejecutar_bronze() -> dict:
    """Ingesta los 6 archivos crudos. Devuelve conteos por tipo y anio."""
    archivos = detectar_archivos(RAW)
    # Un solo valor de _fecha_ingesta para toda la ejecucion
    fecha_ingesta = datetime.now().isoformat(timespec="seconds")
    conteos: dict[str, dict[int, int]] = {tipo: {} for tipo in TIPOS}

    for anio in ANIOS:
        for tipo in TIPOS:
            ruta = archivos[anio][tipo]
            destino = BRONZE / tipo / f"anio={anio}" / "data.parquet"
            conteos[tipo][anio] = ingestar_archivo(ruta, anio, destino, fecha_ingesta=fecha_ingesta)

    return conteos