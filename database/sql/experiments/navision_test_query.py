import os
from dotenv import load_dotenv
import pyodbc

# Configurar las variables de entorno (.env tres niveles arriba)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

# --- helpers mínimos ---
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

# Queries de ejemplo
QUERY_STAFF = """
SELECT
    uo.Cargo as cargo_id,
    u.nombre,
    u.movil
FROM [usuarios obras] uo
LEFT JOIN usuariosnav u ON uo.Usuario = u.userid
WHERE [Nº proyecto] = ?
"""

QUERY_INFO = """
SELECT TOP 10
    o.Description as descripcion,
    o.[Job Posting Group] as tipo,
    o.[Fecha acta de replanteo] as fecha_replanteo,
    UPPER((SELECT TOP 1 c.Name FROM Clientes c WHERE o.[Bill-to Customer No_] = c.No_ AND c.empresa = 1)) AS cliente
FROM [obras ayu] o
"""

if __name__ == "__main__":
    with get_connection() as conn:
        with conn.cursor() as cur:
            print("\n=== Versión del servidor ===")
            cur.execute("SELECT @@VERSION")
            print(cur.fetchone()[0])
            
            print("\n=== Información de obras (TOP 10) ===")
            cur.execute(QUERY_INFO)
            rows = cur.fetchall()
            for row in rows:
                print("\nObra:")
                print(f"  Descripción: {row.descripcion}")
                print(f"  Tipo: {row.tipo}")
                print(f"  Cliente: {row.cliente}")
                print(f"  Fecha replanteo: {row.fecha_replanteo}")
            
            print("\n=== Personal de una obra específica ===")
            codigo_obra = "TU-CODIGO-OBRA"  # Reemplazar con un código real
            cur.execute(QUERY_STAFF, (codigo_obra,))
            staff = cur.fetchall()
            for person in staff:
                print("\nPersonal:")
                print(f"  Cargo: {person.cargo_id}")
                print(f"  Nombre: {person.nombre}")
                print(f"  Móvil: {person.movil}")
