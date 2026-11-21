"""Fase 5: reportes (Tarea 5).

Los reportes 1 a 4 se generan **solo desde gold**. El reporte 5 es la unica
excepcion: se arma con conteos de bronze, silver y cuarentena leyendo solo
metadatos / la columna minima de Parquet (sin cargar las tablas completas).

Salida: ``data/gold/reportes/*.csv`` en UTF-8, separador coma, punto decimal
sin separador de miles, rutas fijas (reemplazo total) y orden determinista
para que dos ejecuciones sean byte a byte identicas.

Estrategia de memoria: en lugar de unir fact completo con las dimensiones,
primero se agrupa por ``fecha_key`` (pocos miles de filas) y recien ahi se
une con ``dim_fecha`` / ``dim_estacion``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from src.bronze import contar_filas_bronze
from src.config import GOLD, REPORTES, SILVER

log = logging.getLogger("reportes")


class VerificacionError(RuntimeError):
    """Se lanza cuando un reporte no puede construirse o no cierra."""


def _guardar_csv(df: pd.DataFrame, nombre: str) -> Path:
    """Escribe un CSV con ruta fija (reemplazo), UTF-8, coma y fin de linea ``\\n``."""
    REPORTES.mkdir(parents=True, exist_ok=True)
    destino = REPORTES / nombre
    # index=False, lineterminator fijo y floats ya redondeados => bytes identicos
    df.to_csv(destino, index=False, encoding="utf-8", lineterminator="\n")
    log.info("Reporte: %s (%d filas)", nombre, len(df))
    return destino


def _unir_dim_fecha(conteo: pd.DataFrame, dim_fecha: pd.DataFrame,
                    columnas: tuple[str, ...] = ("anio", "mes")) -> pd.DataFrame:
    """Agrega anio/mes a un conteo ya agrupado por ``fecha_key``."""
    salida = conteo.merge(dim_fecha[["fecha_key", *columnas]], on="fecha_key",
                          how="left", validate="many_to_one")
    if salida[list(columnas)].isna().any().any():
        raise VerificacionError("Hay fechas de viaje ausentes en dim_fecha")
    return salida


# ---------------------------------------------------------------------------
# Reportes 1-4: desde gold
# ---------------------------------------------------------------------------
def reporte_viajes_por_mes(fact: pd.DataFrame, dim_fecha: pd.DataFrame) -> pd.DataFrame:
    """anio, mes, cantidad_viajes."""
    por_fecha = (fact.groupby("fecha_key", sort=True).size()
                 .rename("cantidad_viajes").reset_index())
    df = _unir_dim_fecha(por_fecha, dim_fecha)
    out = (df.groupby(["anio", "mes"], sort=True)["cantidad_viajes"].sum()
             .reset_index()
             .sort_values(["anio", "mes"], kind="mergesort"))
    return out[["anio", "mes", "cantidad_viajes"]].reset_index(drop=True)


def reporte_top10_estaciones(fact: pd.DataFrame, dim_fecha: pd.DataFrame,
                             dim_estacion: pd.DataFrame) -> pd.DataFrame:
    """anio, ranking, nombre_estacion, cantidad_viajes (viajes **iniciados**).

    Desempate determinista: a igual cantidad de viajes gana el nombre de
    estacion ordenado alfabeticamente.
    """
    por_fecha = (fact.groupby(["fecha_key", "id_estacion_origen"], sort=True).size()
                 .rename("cantidad_viajes").reset_index())
    df = _unir_dim_fecha(por_fecha, dim_fecha, columnas=("anio",))
    conteo = (df.groupby(["anio", "id_estacion_origen"], sort=True)["cantidad_viajes"]
              .sum().reset_index())
    conteo = conteo.merge(dim_estacion[["id_estacion", "nombre_estacion"]],
                          left_on="id_estacion_origen", right_on="id_estacion", how="left")
    # Nombre nulo: se usa un rotulo fijo para que el orden siga siendo determinista
    conteo["nombre_estacion"] = conteo["nombre_estacion"].fillna("SIN NOMBRE")
    conteo = conteo.sort_values(["anio", "cantidad_viajes", "nombre_estacion"],
                                ascending=[True, False, True], kind="mergesort")
    conteo["ranking"] = conteo.groupby("anio").cumcount() + 1
    top = conteo[conteo["ranking"] <= 10]
    return top[["anio", "ranking", "nombre_estacion", "cantidad_viajes"]].reset_index(drop=True)


def reporte_duracion_por_rango_y_genero(fact: pd.DataFrame, dim_usuario: pd.DataFrame) -> pd.DataFrame:
    """rango_etario, genero, cantidad_viajes, duracion_promedio_min.

    Left join contra ``dim_usuario``: los viajes sin usuario (o con usuario no
    encontrado) quedan con DESCONOCIDO en ambas dimensiones.
    """
    df = fact[["id_usuario", "duracion_min"]].merge(
        dim_usuario[["id_usuario", "rango_etario", "genero"]],
        on="id_usuario", how="left", validate="many_to_one",
    )
    df["rango_etario"] = df["rango_etario"].fillna("DESCONOCIDO")
    df["genero"] = df["genero"].fillna("DESCONOCIDO")
    out = (df.groupby(["rango_etario", "genero"], sort=True)
             .agg(cantidad_viajes=("duracion_min", "size"),
                  duracion_promedio_min=("duracion_min", "mean"))
             .reset_index())
    out["duracion_promedio_min"] = out["duracion_promedio_min"].round(1)
    return out.sort_values(["rango_etario", "genero"], kind="mergesort").reset_index(drop=True)


def reporte_viajes_circulares(fact: pd.DataFrame, dim_fecha: pd.DataFrame) -> pd.DataFrame:
    """anio, total_viajes, viajes_circulares, porcentaje_circulares."""
    por_fecha = (fact.groupby(["fecha_key", "es_circular"], sort=True).size()
                 .rename("n").reset_index())
    df = _unir_dim_fecha(por_fecha, dim_fecha, columnas=("anio",))
    df["circulares"] = np.where(df["es_circular"], df["n"], 0)
    out = (df.groupby("anio", sort=True)
             .agg(total_viajes=("n", "sum"), viajes_circulares=("circulares", "sum"))
             .reset_index())
    out["porcentaje_circulares"] = (100.0 * out["viajes_circulares"] / out["total_viajes"]).round(2)
    return out[["anio", "total_viajes", "viajes_circulares", "porcentaje_circulares"]]


# ---------------------------------------------------------------------------
# Reporte 5: excepcion, con conteos de bronze/silver/cuarentena
# ---------------------------------------------------------------------------
def contar_silver(nombre: str) -> dict[int, int]:
    """Conteo por anio leyendo **solo** la columna ``anio_fuente`` del Parquet."""
    serie = pd.read_parquet(SILVER / f"{nombre}.parquet", columns=["anio_fuente"])["anio_fuente"]
    return {int(k): int(v) for k, v in serie.value_counts().items()}


def reporte_calidad_por_anio() -> pd.DataFrame:
    """anio, filas_bronze, filas_silver, filas_cuarentena, porcentaje_cuarentena.

    Refiere a los **viajes** (bronze recorridos = silver viajes + cuarentena).
    """
    bronze = contar_filas_bronze("recorridos")          # solo metadatos
    silver = contar_silver("viajes")                    # solo una columna
    cuarentena = contar_silver("viajes_cuarentena")

    filas = []
    for anio in sorted(bronze):
        b = bronze[anio]
        s = silver.get(anio, 0)
        c = cuarentena.get(anio, 0)
        if b != s + c:
            raise VerificacionError(
                f"Conciliacion rota en {anio}: bronze={b} != silver={s} + cuarentena={c}"
            )
        filas.append({
            "anio": anio,
            "filas_bronze": b,
            "filas_silver": s,
            "filas_cuarentena": c,
            "porcentaje_cuarentena": round(100.0 * c / b, 2) if b else 0.0,
        })
    return pd.DataFrame(filas, columns=["anio", "filas_bronze", "filas_silver",
                                        "filas_cuarentena", "porcentaje_cuarentena"])


# ---------------------------------------------------------------------------
# Orquestacion
# ---------------------------------------------------------------------------
def ejecutar_reportes() -> dict:
    """Genera los cinco CSV en ``data/gold/reportes/``. Devuelve sus rutas."""
    log.info("Reportes: leyendo desde %s", GOLD)
    fact = pd.read_parquet(GOLD / "fact_viajes.parquet")
    dim_fecha = pd.read_parquet(GOLD / "dim_fecha.parquet")
    dim_estacion = pd.read_parquet(GOLD / "dim_estacion.parquet")
    dim_usuario = pd.read_parquet(GOLD / "dim_usuario.parquet")

    reportes = {
        "viajes_por_mes.csv": reporte_viajes_por_mes(fact, dim_fecha),
        "top10_estaciones_por_anio.csv": reporte_top10_estaciones(fact, dim_fecha, dim_estacion),
        "duracion_por_rango_y_genero.csv": reporte_duracion_por_rango_y_genero(fact, dim_usuario),
        "viajes_circulares.csv": reporte_viajes_circulares(fact, dim_fecha),
        "calidad_por_anio.csv": reporte_calidad_por_anio(),
    }
    return {nombre: _guardar_csv(df, nombre) for nombre, df in reportes.items()}