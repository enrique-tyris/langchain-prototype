import os
import argparse
from dotenv import load_dotenv
import pyodbc

# Configurar las variables de entorno (.env tres niveles arriba)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

def _env(k: str):
    v = os.environ.get(k)
    if v is None:
        raise RuntimeError(f"Falta variable de entorno: {k}")
    return v

def get_connection():
    """Conexión mediante FreeTDS ODBC."""
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

def get_venta_firme(job_id: str):
    query = """
    SELECT SUM(dp.[Importe]) AS venta_firme
    FROM [detalles produccion] dp
    WHERE dp.[Nº Proyecto] = ? AND dp.[Tipo] = 0
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (job_id,))
            row = cur.fetchone()
            return row[0]

def get_coste_total(codigo_obra: str):
    query = """
    SELECT [COSTETOTAL]
    FROM [ayu].[dbo].[COSTETOTALOBRAS]
    WHERE [obra] = ?
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (codigo_obra,))
            row = cur.fetchone()
            return row[0]

def main():
    parser = argparse.ArgumentParser(
        description='Consulta márgenes de obra: ingresos certificados vs costes totales'
    )
    parser.add_argument('--id', type=str, default='880', help='ID de obra (por defecto: 880)')
    args = parser.parse_args()

    venta_firme = get_venta_firme(args.id)
    coste_total = get_coste_total(args.id)
    margen = venta_firme - coste_total

    print(f"🔎 MÁRGENES DE OBRA")
    print("=" * 60)
    print(f"🏗️ Obra / Proyecto: {args.id}")
    print(f"💵 Ingresos certificados (venta_firme): {venta_firme:,.2f} €")
    print(f"💸 Costes totales: {coste_total:,.2f} €")
    print(f"📈 Margen: {margen:,.2f} €")

if __name__ == "__main__":
    main()
