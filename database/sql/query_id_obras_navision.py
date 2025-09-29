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

# Query para obtener todos los IDs de obras
QUERY_ALL_WORK_IDS = """
SELECT DISTINCT 
    [Nº proyecto] as id_obra,
    Description as descripcion,
    [Job Posting Group] as tipo_obra
FROM [obras ayu] 
WHERE [Nº proyecto] IS NOT NULL 
ORDER BY [Nº proyecto]
"""

# Query para obtener información de las columnas de la tabla
QUERY_TABLE_COLUMNS = """
SELECT 
    COLUMN_NAME as nombre_columna,
    DATA_TYPE as tipo_dato,
    IS_NULLABLE as permite_nulos,
    CHARACTER_MAXIMUM_LENGTH as longitud_maxima,
    COLUMN_DEFAULT as valor_por_defecto
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'obras ayu'
ORDER BY ORDINAL_POSITION
"""

# Query alternativa para obtener estructura de tabla usando sys.columns
QUERY_TABLE_STRUCTURE = """
SELECT 
    c.name as nombre_columna,
    t.name as tipo_dato,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.column_id
FROM sys.columns c
INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
INNER JOIN sys.tables tb ON c.object_id = tb.object_id
WHERE tb.name = 'obras ayu'
ORDER BY c.column_id
"""

if __name__ == "__main__":
    with get_connection() as conn:
        with conn.cursor() as cur:
            # print("\n=== Versión del servidor ===")
            # cur.execute("SELECT @@VERSION")
            # print(cur.fetchone()[0])
            
            print("\n=== Todas las filas ===")
            cur.execute("""
                SELECT 
                    No_,
                    Description,
                    [Job Posting Group],
                    Estado,
                    CONVERT(varchar, [Last Date Modified], 23) as Last_Date_Modified,
                    CONVERT(varchar, [Creation Date], 23) as Creation_Date
                FROM [obras ayu] 
            """)
            sample_data = cur.fetchall()
            print(f"Mostrando {len(sample_data)} registros:")
            for row in sample_data:
                print(f"No_={row[0]} | Desc={row[1]} | Group={row[2]} | Estado={row[3]} | Mod={row[4]} | Creacion={row[5]}")