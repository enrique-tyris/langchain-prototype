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

def get_precio_obra(codigo_obra: str):
    """
    Obtiene el presupuesto vigente+IVA de una obra,
    filtrando por columna [No_] y grupos permitidos.
    """
    query = """
    SELECT o.[Presupuesto Vigente+IVA] AS presupuesto
    FROM [obras ayu] o
    WHERE o.[No_] = ?
      AND o.[Job Posting Group] IN ('1:EDIF RES', '2:EDIF NOR', '4:O CIVIL')
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (codigo_obra,))
            row = cur.fetchone()
            return row[0]

def main():
    parser = argparse.ArgumentParser(
        description="Precio de la obra: presupuesto vigente con IVA (incluyendo desviaciones)."
    )
    parser.add_argument('--id', type=str, default='880', help='Código de obra (por defecto: 880)')
    args = parser.parse_args()

    precio = get_precio_obra(args.id)

    print("🔎 PRECIO DE LA OBRA (Presupuesto Vigente + IVA)")
    print("=" * 60)
    print(f"🏗️ Obra: {args.id}")
    print(f"💰 Precio total (con desviaciones): {precio:,.2f} €")

if __name__ == "__main__":
    main()
