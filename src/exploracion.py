"""Fase 1: exploracion de los archivos crudos (Tarea 1).

No forma parte de ``main.py``. Se ejecuta manualmente:

    python -m src.exploracion

Para cada anio y tipo de archivo detecta: nombre del archivo dentro del ZIP,
separador, encoding, columnas, formato de fecha/hora, presencia y unidad de
duracion, presencia de id de usuario, y cuenta el total de filas leyendo por
chunks (sin cargar todo en memoria). Ademas muestra value_counts() de genero
y rangos de edad para los archivos de usuarios.
"""

from __future__ import annotations

import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import ANIOS, CHUNKSIZE, NROWS, detectar_archivos

# Formatos de fecha/hora candidatos, en orden de preferencia
FORMATOS_CANDIDATOS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%d-%m-%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%d",
    # Detectan fecha_alta y hora_alta de usuarios (C-04)
    "%d-%m-%y",
    "%I:%M:%S %p",
    "%H:%M:%S",
)

SEPARADORES = (",", ";", "\t", "|")


# ---------------------------------------------------------------------------
# Deteccion de formato
# ---------------------------------------------------------------------------
def listar_zip(ruta_zip: Path) -> list[str]:
    """Lista los nombres de archivo contenidos en un ZIP."""
    with zipfile.ZipFile(ruta_zip) as zf:
        return [i.filename for i in zf.infolist() if not i.is_dir()]


def elegir_csv_interno(nombres: list[str]) -> str:
    """Elige el CSV de recorridos correcto si el ZIP trae mas de un archivo."""
    csvs = [n for n in nombres if n.lower().endswith(".csv")]
    if not csvs:
        raise ValueError(f"El ZIP no contiene ningun CSV: {nombres}")
    preferidos = [n for n in csvs if any(k in n.lower() for k in ("recorrido", "viaje", "trip"))]
    return (preferidos or csvs)[0]


def detectar_encoding(muestra: bytes) -> str:
    """Prueba UTF-8 con y sin BOM; si falla, asume latin-1."""
    for enc in ("utf-8-sig", "utf-8"):
        try:
            muestra.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def detectar_separador(texto: str) -> str:
    """Elige el separador que produce la mayor cantidad de columnas constante."""
    mejor, mejor_puntaje = ",", -1.0
    lineas = [l for l in texto.splitlines() if l.strip()][:50]
    if not lineas:
        return ","
    for sep in SEPARADORES:
        conteos = [l.count(sep) for l in lineas]
        if max(conteos) == 0:
            continue
        # Constante en casi todas las lineas y con al menos 2 columnas
        moda = Counter(conteos).most_common(1)[0]
        puntaje = moda[1] / len(conteos) * moda[0]
        if puntaje > mejor_puntaje:
            mejor, mejor_puntaje = sep, puntaje
    return mejor


def adivinar_formato_fecha(valores: list[str]) -> str | None:
    """Devuelve el formato de fecha/hora que explica todos los valores muestreados."""
    muestras = [v for v in valores if v and str(v).strip()][:100]
    if not muestras:
        return None
    for fmt in FORMATOS_CANDIDATOS:
        ok = True
        for v in muestras:
            try:
                datetime.strptime(v.strip(), fmt)
            except ValueError:
                ok = False
                break
        if ok:
            return fmt
    return None


def _es_numerico(texto: str) -> bool:
    try:
        float(texto)
        return True
    except (TypeError, ValueError):
        return False


def detectar_duracion(cols: list[str], muestra: pd.DataFrame) -> tuple[str | None, str, str | None]:
    """Busca una columna de duracion e intenta adivinar su unidad.

    Devuelve ``(columna, unidad, formato_observado)``.
    """
    candidatas = [
        c for c in cols
        if any(k in c.lower() for k in ("duracion", "duration", "tiempo", "segundo", "minuto"))
    ]
    if not candidatas:
        return None, "no trae", None
    col = candidatas[0]
    bajo = col.lower()
    if "min" in bajo:
        unidad = "minutos"
    elif "seg" in bajo or "sec" in bajo:
        unidad = "segundos"
    else:
        unidad = "desconocida"
    valores = [v for v in muestra[col].astype(str).tolist() if v.strip() and _es_numerico(v.replace(".", "").replace(",", "."))]
    if valores:
        nums = sorted(float(v.replace(".", "").replace(",", ".")) for v in valores)
        mediana = nums[len(nums) // 2]
        if unidad == "desconocida":
            # Heuristica: duraciones de viaje en segundos suelen ser cientos/miles
            unidad = "segundos (supuesto por magnitud)" if mediana > 30 else "minutos (supuesto por magnitud)"
        observado = f"ejemplo={valores[0]!r}, mediana={mediana:g}"
    else:
        observado = "sin valores numericos en la muestra"
    return col, unidad, observado


def detectar_id_usuario(cols: list[str]) -> str | None:
    """Devuelve la columna de id de usuario solo si el nombre lo dice.

    Antes cualquier columna con "usuario" valia (p. ej. ``genero_usuario`` en
    los recorridos 2016) y el informe contradecia DECISIONES.md. Ahora el
    nombre debe ser exactamente ``id_usuario``/``ID_usuario``/``user_id``
    (sin separadores, en minusculas) para que cuente como id.
    """
    for c in cols:
        if c.lower().replace("_", "") in ("idusuario", "userid"):
            return c
    return None


def columnas_fecha(cols: list[str]) -> list[str]:
    return [
        c for c in cols
        if any(k in c.lower() for k in ("fecha", "hora", "date", "time", "inicio", "fin"))
    ]


# ---------------------------------------------------------------------------
# Lectura perezosa (solo las primeras filas) y conteo por chunks
# ---------------------------------------------------------------------------
def leer_muestra(ruta: Path, sep: str = "", encoding: str = "") -> tuple[pd.DataFrame, str, str, str | None]:
    """Lee solo ``NROWS`` filas detectando separador/encoding y, en ZIP, el CSV interno."""
    if ruta.suffix.lower() == ".zip":
        interno = elegir_csv_interno(listar_zip(ruta))
    else:
        interno = None

    if not sep or not encoding:
        if interno:
            with zipfile.ZipFile(ruta) as zf:
                crudo = zf.open(interno).read(65_536)
        else:
            with open(ruta, "rb") as fh:
                crudo = fh.read(65_536)
        encoding = encoding or detectar_encoding(crudo)
        sep = sep or detectar_separador(crudo.decode(encoding, errors="replace"))

    df = pd.read_csv(
        ruta,
        sep=sep,
        encoding=encoding,
        dtype=str,
        keep_default_na=False,
        nrows=NROWS,
        compression="zip" if interno else None,
    )
    return df, sep, encoding, interno


def contar_filas(ruta: Path, sep: str, encoding: str) -> int:
    """Cuenta el total de filas leyendo por ``chunksize`` (sin cargar todo en memoria)."""
    interno = ruta.suffix.lower() == ".zip"
    total = 0
    lector = pd.read_csv(
        ruta,
        sep=sep,
        encoding=encoding,
        dtype=str,
        keep_default_na=False,
        chunksize=CHUNKSIZE,
        compression="zip" if interno else None,
        usecols=[0],  # solo la primera columna basta para contar filas
    )
    for chunk in lector:
        total += len(chunk)
    return total


# ---------------------------------------------------------------------------
# Exploracion por tipo
# ---------------------------------------------------------------------------
def explorar_recorridos(ruta: Path) -> dict:
    """Detecta el formato de un ZIP de recorridos y cuenta sus filas."""
    df, sep, enc, interno = leer_muestra(ruta)
    cols = list(df.columns)

    fechas: dict[str, str | None] = {}
    for col in columnas_fecha(cols):
        fechas[col] = adivinar_formato_fecha(df[col].astype(str).tolist())

    col_dur, unidad, obs_dur = detectar_duracion(cols, df)
    col_user = detectar_id_usuario(cols)

    return {
        "archivo": ruta.name,
        "interno": interno or "-",
        "separador": repr(sep),
        "encoding": enc,
        "columnas": cols,
        "fechas": fechas,
        "duracion_col": col_dur,
        "duracion_unidad": unidad,
        "duracion_obs": obs_dur,
        "id_usuario": col_user or "no trae",
        "filas": contar_filas(ruta, sep, enc),
    }


def explorar_usuarios(ruta: Path) -> dict:
    """Detecta el formato del CSV de usuarios, cuenta filas y resume genero/edad."""
    df, sep, enc, _ = leer_muestra(ruta)
    cols = list(df.columns)

    fechas: dict[str, str | None] = {}
    for col in columnas_fecha(cols):
        fechas[col] = adivinar_formato_fecha(df[col].astype(str).tolist())

    col_user = detectar_id_usuario(cols)
    col_gen = next((c for c in cols if any(k in c.lower() for k in ("genero", "género", "gender", "sexo"))), None)
    col_edad = next((c for c in cols if "edad" in c.lower() or "age" in c.lower()), None)

    generos: Counter = Counter()
    edades: list[int] = []
    altas: Counter = Counter()
    if col_gen:
        for chunk in pd.read_csv(ruta, sep=sep, encoding=enc, dtype=str, keep_default_na=False,
                                 chunksize=CHUNKSIZE, usecols=[col_gen]):
            generos.update(chunk[col_gen].astype(str))
    if col_edad:
        for chunk in pd.read_csv(ruta, sep=sep, encoding=enc, dtype=str, keep_default_na=False,
                                 chunksize=CHUNKSIZE, usecols=[col_edad]):
            for v in chunk[col_edad]:
                v = str(v).strip().replace(".", "").replace(",", ".")
                if _es_numerico(v):
                    edades.append(int(float(v)))
    if col_user:
        col_fecha_alta = next((c for c in cols if "alta" in c.lower()), None)
        if col_fecha_alta:
            for chunk in pd.read_csv(ruta, sep=sep, encoding=enc, dtype=str, keep_default_na=False,
                                     chunksize=CHUNKSIZE, usecols=[col_fecha_alta]):
                altas.update(chunk[col_fecha_alta].astype(str).str[:4])

    edades_sorted = sorted(edades)
    rango_edad = (
        f"min={edades_sorted[0]}, max={edades_sorted[-1]}, n={len(edades_sorted)}"
        if edades_sorted else "sin columna de edad numerica"
    )

    return {
        "archivo": ruta.name,
        "separador": repr(sep),
        "encoding": enc,
        "columnas": cols,
        "fechas": fechas,
        "id_usuario": col_user or "no trae",
        "generos": dict(generos.most_common()),
        "col_genero": col_gen or "-",
        "col_edad": col_edad or "-",
        "rango_edad": rango_edad,
        "altas_por_anio": dict(sorted(altas.items())),
        "filas": contar_filas(ruta, sep, enc),
    }


# ---------------------------------------------------------------------------
# Reporte
# ---------------------------------------------------------------------------
def _tabla_md(titulo: str, filas: list[tuple[str, dict]]) -> str:
    """Imprime una tabla markdown con una fila por anio para un campo dado."""
    anios = ", ".join(str(a) for a in ANIOS)
    lineas = [f"### {titulo}", "", f"| Aspecto a revisar | {anios} |", "|---" * (len(ANIOS) + 1) + "|"]
    claves = filas[0][1].keys()
    for clave in claves:
        celdas = [str(d.get(clave, "")) for _, d in filas]
        lineas.append(f"| {clave} | " + " | ".join(celdas) + " |")
    return "\n".join(lineas)


def generar_informe() -> str:
    """Explora los 6 archivos y devuelve las dos tablas markdown para DECISIONES.md."""
    archivos = detectar_archivos()
    recorridos = {a: explorar_recorridos(archivos[a]["recorridos"]) for a in ANIOS}
    usuarios = {a: explorar_usuarios(archivos[a]["usuarios"]) for a in ANIOS}

    filas_rec = []
    for a in ANIOS:
        r = recorridos[a]
        filas_rec.append((a, {
            "Nombre del archivo dentro del ZIP": r["interno"],
            "Separador de columnas": f"{r['separador']} (encoding {r['encoding']})",
            "Nombres de las columnas": ", ".join(str(c) for c in r["columnas"]),
            "Formato de fecha y hora": "; ".join(f"{k} → {v}" for k, v in r["fechas"].items()) or "sin columnas de fecha",
            "¿Trae duración? ¿En qué unidad?": (
                f"sí: `{r['duracion_col']}` en {r['duracion_unidad']} ({r['duracion_obs']})"
                if r["duracion_col"] else "no"
            ),
            "¿Trae id de usuario?": r["id_usuario"],
            "Cantidad total de filas": f"{r['filas']:,}".replace(",", "."),
        }))

    filas_usu = []
    for a in ANIOS:
        u = usuarios[a]
        filas_usu.append((a, {
            "Nombre del archivo": u["archivo"],
            "Separador de columnas": f"{u['separador']} (encoding {u['encoding']})",
            "Nombres de las columnas": ", ".join(str(c) for c in u["columnas"]),
            "Formato de fecha y hora": "; ".join(f"{k} → {v}" for k, v in u["fechas"].items()) or "sin columnas de fecha",
            "Género (value_counts)": ", ".join(f"{k}={v}" for k, v in u["generos"].items()) or "-",
            "Edad": f"{u['col_edad']}: {u['rango_edad']}",
            "Altas por año": ", ".join(f"{k}={v}" for k, v in u["altas_por_anio"].items()) or "-",
            "¿Trae id de usuario?": u["id_usuario"],
            "Cantidad total de filas": f"{u['filas']:,}".replace(",", "."),
        }))

    return (
        _tabla_md("Recorridos (viajes)", filas_rec)
        + "\n\n"
        + _tabla_md("Usuarios", filas_usu)
    )


def main(ruta_salida: str | None = None) -> None:
    """Genera el informe; lo escribe en `ruta_salida` (UTF-8) o lo imprime."""
    informe = generar_informe()
    if ruta_salida:
        Path(ruta_salida).write_text(informe, encoding="utf-8")
        print(f"Informe escrito en {ruta_salida}")
    else:
        import sys
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        print(informe)


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else None)