import os
import argparse
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

def get_certificacion_parcial(job_id: str):
    """Obtiene la certificación parcial total para un Job No. específico."""
    query = """
    SELECT
       SUM(m.[Total Price (LCY)]) AS certificacion
    FROM movproyecto m
    WHERE m.[Job No_] = ? AND empresa = 1
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (job_id,))
            result = cur.fetchone()
            return result[0]

def main():
    parser = argparse.ArgumentParser(
        description='Consultar certificación parcial total para un Job No. desde Navision'
    )
    parser.add_argument('--id', type=str, default='880', help='ID de obra (por defecto: 880)')
    
    args = parser.parse_args()
    
    print(f"🔍 CONSULTA DE CERTIFICACIÓN PARCIAL PARA OBRA ID: {args.id}")
    print("=" * 70)
    print("📊 Base de datos: Navision (SQL Server)")
    print("📋 Tabla: movproyecto")
    
    try:
        certificacion = get_certificacion_parcial(args.id)
        
        print(f"\n📊 Resultado:")
        print(f"   🏗️ ID de obra: {args.id}")
        
        if certificacion is not None:
            print(f"   💰 Certificación parcial total: {certificacion:,.2f} €")
            print(f"\n✅ Se encontraron datos de certificación para la obra {args.id}")
        else:
            print("   ⚠️ No se encontraron datos de certificación")
            print("   💡 Verifica que el Job No_ existe y tiene movimientos en movproyecto")
            
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print("💡 Verifica las credenciales de Navision en el archivo .env")

if __name__ == "__main__":
    main()
