#!/usr/bin/env python3
"""
Lee un Excel de masters vía SFTP (Paramiko) usando variables de entorno
y extrae:
- Costes Directos  -> Fila 33, columnas M-N-O (A/P/O)
- Costes Indirectos-> Fila 38, columnas M-N-O (A/P/O)
de la hoja "FICHA OBRA mes".

ENV obligatorias:
- MASTERS_HOST, MASTERS_USER, MASTERS_PASSWORD, MASTERS_FOLDER
Opcionales:
- MASTER_FILE_PATH  -> ruta completa del XLS remoto si ya la sabes
- FICHA_SHEET       -> por defecto "FICHA OBRA mes"
"""

import os
import re
import sys
import warnings
from io import BytesIO
from pathlib import PurePosixPath

import pandas as pd
import paramiko
from dotenv import load_dotenv

load_dotenv()

# Silenciar warning de openpyxl sobre Conditional Formatting
warnings.filterwarnings(
    "ignore",
    message="Conditional Formatting extension is not supported and will be removed",
    category=UserWarning,
)

# Variables
DEFAULT_SHEET = "FICHA OBRA mes" # hoja del XLS por defecto
MASTER_FILE_PATH = "/datos/OBRAS/MASTER/25-06/EDIFICACION/25 06 MASTER 880 94 VPO PEÑOTA ORTUELLA.xlsm" # ruta del XLS a leer

MASTERS_HOST = os.environ.get("MASTERS_HOST")
MASTERS_USER = os.environ.get("MASTERS_USER")
MASTERS_PASSWORD = os.environ.get("MASTERS_PASSWORD")
MASTERS_FOLDER = os.environ.get("MASTERS_FOLDER")

# Fila/columnas objetivo
ROW_DIRECTOS = 35
ROW_INDIRECTOS = 38
COLS = ["M", "N", "O"]   # A/P/O


def fail(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def sftp_connect():
    if not all([MASTERS_HOST, MASTERS_USER, MASTERS_PASSWORD, MASTERS_FOLDER]):
        fail("Faltan variables: MASTERS_HOST, MASTERS_USER, MASTERS_PASSWORD, MASTERS_FOLDER")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        MASTERS_HOST,
        username=MASTERS_USER,
        password=MASTERS_PASSWORD,
        timeout=15,
        banner_timeout=15,
    )
    return ssh, ssh.open_sftp()

def excel_letters(n: int):
    """Devuelve nombres de columnas estilo Excel: A, B, ..., Z, AA, AB, ..."""
    letters = []
    i = 0
    while len(letters) < n:
        s, k = "", i
        while True:
            s = chr(k % 26 + 65) + s
            k = k // 26 - 1
            if k < 0:
                break
        letters.append(s)
        i += 1
    return letters


def read_sheet_positioned(sftp, remote_path: str, sheet_name: str) -> pd.DataFrame:
    """Lee la hoja con header=None y etiqueta columnas como Excel (A,B,...) e índice 1-based."""
    with sftp.open(remote_path, "rb") as f:
        data = f.read()

    bio = BytesIO(data)
    suffix = PurePosixPath(remote_path).suffix.lower()
    engine = "xlrd" if suffix == ".xls" else None

    df = pd.read_excel(bio, sheet_name=sheet_name, header=None, engine=engine)
    df.columns = excel_letters(df.shape[1])  # A, B, C, ...
    df.index = df.index + 1                  # 1-based como Excel
    return df


def get_row_MNO(df: pd.DataFrame, row: int, header_offset: int = 0) -> dict:
    """
    Devuelve dict con A/P/O (M,N,O) para la fila dada.

    NOTA: La hoja tiene un cabecero de 3 líneas que desplaza la tabla real.
    Por eso sumamos `header_offset` (por defecto 3) a la fila solicitada.
    Ej.: si pides row=33, realmente leemos la fila 36 (33 + 3).
    """
    # Fila real en el DataFrame teniendo en cuenta el cabecero
    excel_row = row + header_offset  # <-- sumamos 3 por el cabecero

    # Leer columnas M, N y O en esa fila
    vals = df.loc[excel_row, ["M", "N", "O"]].copy()

    # Guardamos los valores crudos para informar tal cual vienen de la hoja
    raw = vals.to_dict()

    # Normalizamos a número (NaN -> 0) para poder operar
    num = pd.to_numeric(vals, errors="coerce").fillna(0)

    # Mapeamos a A/P/O (Actual / Previsión / Objetivo)
    clean = {"A": float(num["M"]), "P": float(num["N"]), "O": float(num["O"])}

    return raw, clean

def _fmt(x: float) -> str:
    """Formato bonito con separador de miles (sin decimales si no hacen falta)."""
    if float(x).is_integer():
        return f"{x:,.0f}"
    return f"{x:,.2f}"


def main():
    ssh = None
    sftp = None
    try:
        ssh, sftp = sftp_connect()

        target = MASTER_FILE_PATH # ruta del XLS a leer

        # Cargar hoja
        df = read_sheet_positioned(sftp, target, DEFAULT_SHEET)

        # Extraer Costes Directos (Row 33) y Costes Indirectos (Row 38)
        raw_dir, clean_dir = get_row_MNO(df, ROW_DIRECTOS)
        raw_ind, clean_ind = get_row_MNO(df, ROW_INDIRECTOS)

        # Si quieres multiplicar por 1000 como en tu reader, descomenta:
        # clean_dir = {k: v * 1000 for k, v in clean_dir.items()}
        # clean_ind = {k: v * 1000 for k, v in clean_ind.items()}

        # --- PRINTS BASE ---
        print(f"✔️ Abierto: {target}")
        print(f"Hoja: {DEFAULT_SHEET}")

        # --- INFORME CLARO ---
        print("\n— Costes Directos —")
        print("Definición: Materiales, mano de obra y subcontratistas imputables a partidas.")
        print(f"Rango leído: M{ROW_DIRECTOS}:O{ROW_DIRECTOS}  (A/P/O = Actual / Previsión / Objetivo)")
        print(f"Valores crudos:  M{ROW_DIRECTOS}={raw_dir['M']!r}  N{ROW_DIRECTOS}={raw_dir['N']!r}  O{ROW_DIRECTOS}={raw_dir['O']!r}")
        print("Valores normalizados (numéricos, NaN→0):")
        print(f"  • Actual     (M{ROW_DIRECTOS}): {_fmt(clean_dir['A'])}")
        print(f"  • Previsión  (N{ROW_DIRECTOS}): {_fmt(clean_dir['P'])}")
        print(f"  • Objetivo   (O{ROW_DIRECTOS}): {_fmt(clean_dir['O'])}")

        print("\n— Costes Indirectos —")
        print("Definición: Costes de estructura de obra: oficina, suministros, seguros.")
        print(f"Rango leído: M{ROW_INDIRECTOS}:O{ROW_INDIRECTOS}  (A/P/O = Actual / Previsión / Objetivo)")
        print(f"Valores crudos:  M{ROW_INDIRECTOS}={raw_ind['M']!r}  N{ROW_INDIRECTOS}={raw_ind['N']!r}  O{ROW_INDIRECTOS}={raw_ind['O']!r}")
        print("Valores normalizados (numéricos, NaN→0):")
        print(f"  • Actual     (M{ROW_INDIRECTOS}): {_fmt(clean_ind['A'])}")
        print(f"  • Previsión  (N{ROW_INDIRECTOS}): {_fmt(clean_ind['P'])}")
        print(f"  • Objetivo   (O{ROW_INDIRECTOS}): {_fmt(clean_ind['O'])}")

        # Resumen final tipo “tabla” simple
        print("\nResumen:")
        print(f"Directos    -> A: {_fmt(clean_dir['A'])} | P: {_fmt(clean_dir['P'])} | O: {_fmt(clean_dir['O'])}")
        print(f"Indirectos  -> A: {_fmt(clean_ind['A'])} | P: {_fmt(clean_ind['P'])} | O: {_fmt(clean_ind['O'])}")

    finally:
        try:
            if sftp:
                sftp.close()
        finally:
            if ssh:
                ssh.close()


if __name__ == "__main__":
    main()
