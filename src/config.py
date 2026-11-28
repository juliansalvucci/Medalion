"""Configuracion central del pipeline: rutas, anios y deteccion de archivos.

Los nombres exactos de los archivos los define el usuario al descargarlos,
por lo que se detectan por patron (anio + "recorrido" / "usuario") y nunca
se asumen.
"""

from __future__ import annotations

import re
from pathlib import Path

# ---------------------------------------------------------------------------
# Rutas fijas del proyecto (estructura de carpetas indicada en el enunciado)
# ---------------------------------------------------------------------------
BASE = Path(__file__).resolve().parent.parent

DATA = BASE / "data"
RAW = DATA / "raw"
BRONZE = DATA / "bronze"
SILVER = DATA / "silver"
GOLD = DATA / "gold"
REPORTES = GOLD / "reportes"

# Anios exigidos por el enunciado
ANIOS = (2016, 2019, 2024)

# Tipos de archivo (clave para detectarlos por nombre)
TIPOS = ("recorridos", "usuarios")

# Metadatos que bronze agrega a cada tabla
COL_ARCHIVO_ORIGEN = "_archivo_origen"
COL_FECHA_INGESTA = "_fecha_ingesta"
COL_ANIO = "_anio"
METADATOS = (COL_ARCHIVO_ORIGEN, COL_FECHA_INGESTA, COL_ANIO)

# Filas por chunk en lecturas perezosas (bronze y exploracion)
CHUNKSIZE = 200_000

# Limite de filas para la exploracion de formato
NROWS = 1000


class ArchivoFaltante(RuntimeError):
    """Se lanza cuando falta un archivo de data/raw/ que el usuario debe descargar."""


class VerificacionError(RuntimeError):
    """Se lanza cuando una verificacion del pipeline no se cumple.

    Vive en config para que bronze pueda usarla sin importar silver (silver
    importa de bronze: la definicion local crearia un ciclo).
    """


def _normalizar(texto: str) -> str:
    """Minusculas y sin guiones/espacios, para comparar nombres de archivo."""
    return re.sub(r"[\s_\-]+", "", texto.lower())


def detectar_archivos(raw: Path = RAW, anios: tuple[int, ...] = ANIOS) -> dict[int, dict[str, Path]]:
    """Detecta en ``raw`` un archivo por anio y tipo por patron sobre el nombre.

    Devuelve ``{anio: {"recorridos": Path, "usuarios": Path}}``.
    Lanza :class:`ArchivoFaltante` con un mensaje claro si falta alguno.
    """
    if not raw.exists():
        raise ArchivoFaltante(f"No existe el directorio de datos crudos: {raw}")

    encontrados: dict[int, dict[str, Path]] = {a: {} for a in anios}
    pendientes: list[str] = []

    for ruta in sorted(raw.iterdir()):
        if not ruta.is_file() or ruta.suffix.lower() not in {".zip", ".csv"}:
            continue
        nombre = _normalizar(ruta.stem)
        for anio in anios:
            if str(anio) not in nombre:
                continue
            for tipo in TIPOS:
                # "recorridos" se acepta tambien como "recorrido" (singular)
                clave = tipo[:-1] if tipo.endswith("s") else tipo
                if clave in nombre and tipo not in encontrados[anio]:
                    encontrados[anio][tipo] = ruta

    for anio in anios:
        for tipo in TIPOS:
            if tipo not in encontrados[anio]:
                pendientes.append(
                    f"- {anio}: archivo de '{tipo}' (debe contener '{anio}' y '{tipo}' en el nombre)"
                )

    if pendientes:
        raise ArchivoFaltante(
            "Faltan archivos en data/raw/ (deben descargarse del portal de datos de la "
            "Ciudad de Buenos Aires, sin editarlos ni descomprimirlos):\n" + "\n".join(pendientes)
        )

    return encontrados