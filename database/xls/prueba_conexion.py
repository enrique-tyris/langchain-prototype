#!/usr/bin/env python3
"""
Abre un Excel de masters vía SFTP (Paramiko) usando variables de entorno.
- MASTERS_HOST, MASTERS_USER, MASTERS_PASSWORD, MASTERS_FOLDER  (obligatorias)
- D
"""

import os
import re
import sys
from io import BytesIO
from pathlib import PurePosixPath

import pandas as pd
import paramiko
from stat import S_ISDIR, S_ISREG
from dotenv import load_dotenv
load_dotenv()

# mismo patrón que en onayu searcher.py
REGEXP = re.compile(r"\d{2} \d{2} MASTER \d{3} .*\.xls.?", re.IGNORECASE)
DEFAULT_SHEET = "FICHA OBRA mes" # hoja del XLS por defecto
MASTER_FILE_PATH = "/datos/OBRAS/MASTER/25-06/EDIFICACION/25 06 MASTER 880 94 VPO PEÑOTA ORTUELLA.xlsm" # ruta del XLS a leer

MASTERS_HOST = os.environ.get("MASTERS_HOST")
MASTERS_USER = os.environ.get("MASTERS_USER")
MASTERS_PASSWORD = os.environ.get("MASTERS_PASSWORD")
MASTERS_FOLDER = os.environ.get("MASTERS_FOLDER")

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

def find_first_master(sftp, root: str) -> str | None:
    """Busca recursivamente el primer fichero que cumpla el regex REGEXP."""
    stack = [root.rstrip("/")]
    while stack:
        parent = stack.pop()
        for entry in sftp.listdir_attr(parent):
            path = f"{parent}/{entry.filename}"
            if S_ISDIR(entry.st_mode):
                stack.append(path)
            elif S_ISREG(entry.st_mode) and REGEXP.match(entry.filename):
                return path
    return None

def read_excel_from_sftp(sftp, remote_path: str, prefer_sheet: str = DEFAULT_SHEET) -> pd.DataFrame:
    # leemos todo a memoria para evitar problemas de reposicionamiento
    with sftp.open(remote_path, "rb") as f:
        data = f.read()

    bio = BytesIO(data)
    suffix = PurePosixPath(remote_path).suffix.lower()
    engine = "xlrd" if suffix == ".xls" else None

    with pd.ExcelFile(bio, engine=engine) as xls:
        sheet = prefer_sheet if prefer_sheet in xls.sheet_names else xls.sheet_names[0]
        bio.seek(0)
        df = pd.read_excel(bio, sheet_name=sheet, engine=engine)
        print(f"✔️ Abierto: {remote_path}")
        print(f"   Hoja usada: {sheet}")
        print(f"   Todas las hojas: {xls.sheet_names}")
        return df

def main():
    ssh = None
    sftp = None
    try:
        ssh, sftp = sftp_connect()

        target = MASTER_FILE_PATH
        if not target:
            print(f"Buscando master en: {MASTERS_FOLDER}")
            target = find_first_master(sftp, MASTERS_FOLDER)

        if not target:
            fail(f"No se encontró ningún master bajo {MASTERS_FOLDER} que cumpla el patrón '{REGEXP.pattern}'")

        df = read_excel_from_sftp(sftp, target, DEFAULT_SHEET)

        # salida mínima para verificar
        print("\nPrimeras filas:")
        print(df.head())
        print(f"\nShape: {df.shape}")

    finally:
        try:
            if sftp: sftp.close()
        finally:
            if ssh: ssh.close()

if __name__ == "__main__":
    main()
