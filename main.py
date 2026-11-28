"""Pipeline Ecobici: Bronze -> Silver -> Gold -> Reportes.

Ejecucion de punta a punta:

    python main.py

Requisitos: solo ``data/raw/`` poblado con los 6 archivos descargados.
Opcional: ``python main.py --desde silver`` para reprocesar desde una capa.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from src.config import BRONZE, GOLD, REPORTES, RAW, SILVER, ArchivoFaltante, detectar_archivos
from src.bronze import ejecutar_bronze
from src.gold import ejecutar_gold
from src.reportes import ejecutar_reportes
from src.silver import ejecutar_silver

log = logging.getLogger("pipeline")

# Orden fijo del pipeline. Cada etapa es una funcion ejecutar_*().
ETAPAS = (
    ("bronze", ejecutar_bronze),
    ("silver", ejecutar_silver),
    ("gold", ejecutar_gold),
    ("reportes", ejecutar_reportes),
)


def preparar_directorios() -> None:
    """Crea los directorios de salida si no existen (nunca toca data/raw/)."""
    for ruta in (BRONZE, SILVER, GOLD, REPORTES):
        ruta.mkdir(parents=True, exist_ok=True)


def validar_materias_primas() -> dict:
    """Valida al inicio que existan los 6 archivos de data/raw/."""
    archivos = detectar_archivos(RAW)
    for anio, por_tipo in archivos.items():
        for tipo, ruta in por_tipo.items():
            log.info("Materia prima %s %s: %s (%.1f MB)", anio, tipo, ruta.name,
                     ruta.stat().st_size / 1_048_576)
    return archivos


def ejecutar(desde: str = "bronze") -> dict:
    """Ejecuta las etapas en orden desde ``desde``. Falla explicita ante cualquier error."""
    orden = [nombre for nombre, _ in ETAPAS]
    resultados: dict[str, object] = {}
    for nombre, funcion in ETAPAS[orden.index(desde):]:
        tiempo = time.perf_counter()
        log.info("=== Etapa %s: inicio ===", nombre)
        resultados[nombre] = funcion()
        log.info("=== Etapa %s: fin (%.1f s) ===", nombre, time.perf_counter() - tiempo)
    return resultados


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline Ecobici (Bronze/Silver/Gold)")
    parser.add_argument("--desde", choices=[n for n, _ in ETAPAS], default="bronze",
                        help="Reprocesa desde esta capa (por defecto: pipeline completo)")
    args = parser.parse_args(argv)

    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    try:
        preparar_directorios()
        # data/raw/ solo hace falta para bronze: las capas posteriores leen
        # de las capas que ya existen, asi que --desde silver|gold|reportes
        # no la valida (C-05).
        if args.desde == "bronze":
            validar_materias_primas()
        resultados = ejecutar(args.desde)
    except ArchivoFaltante as exc:
        log.error("%s", exc)
        return 1
    except Exception as exc:  # cualquier falla de verificacion corta con codigo != 0
        log.exception("El pipeline fallo: %s", exc)
        return 1

    log.info("Pipeline OK. Etapas ejecutadas desde '%s': %s", args.desde,
             ", ".join(resultados))
    return 0


if __name__ == "__main__":
    sys.exit(main())