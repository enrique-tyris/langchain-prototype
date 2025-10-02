import os
from dotenv import load_dotenv
import pyodbc

# Carga .env desde la raíz del proyecto
PROJECT_ROOT = os.path.dirname(os.path.abspath(os.path.join(__file__, "..", "..")))
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")
load_dotenv(ENV_PATH)

def _env(k: str) -> str:
    v = os.environ.get(k)
    if not v:
        raise RuntimeError(f"Falta variable de entorno: {k}")
    return v

def get_connection() -> pyodbc.Connection:
    """
    Conexión a SQL Server (FreeTDS / ODBC).
    Devuelve un objeto DB-API 2.0 con .cursor(), .execute(), .fetchall().
    """
    host = _env("NAVISION_DB_SERVICE_HOST")
    port = os.environ.get("NAVISION_DB_SERVICE_PORT", "1433")
    db   = _env("NAVISION_DB_NAME")
    uid  = _env("NAVISION_DB_USERNAME")
    pwd  = _env("NAVISION_DB_PASSWORD")
    tls  = os.environ.get("AYUDB_FREETDS_SSLPROTO", "tls1")

    conn_str = (
        "DRIVER={FreeTDS};"
        f"Server={host};Port={port};DATABASE={db};"
        f"UID={uid};PWD={pwd};"
        "TDS_Version=7.2;"
        "Encrypt=yes;"
        f"sslprotocol={tls};"
        "ClientCharset=UTF-8;"
    )
    return pyodbc.connect(conn_str, timeout=10)
