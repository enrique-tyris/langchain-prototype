import os
import argparse
from dotenv import load_dotenv
import pyodbc
from datetime import datetime

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

def get_fechas_obra(obra_id):
    """Obtiene las fechas importantes de una obra específica."""
    query = """
    SELECT
        o.[Fecha acta recep_ definitiva] as recepcion,
        o.[Fecha adjudicación] as adjudicacion,
        o.[Fecha firma contrato] as firma_contrato,
        o.[Fecha acta de replanteo] as replanteo,
        o.[Fecha Fin Vigente] as fin_contrato
    FROM [obras ayu] o
    WHERE o.No_ = ?
    """
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (obra_id,))
            return cur.fetchone()

def _fmt_fecha(f):
    """Formatea fecha, devolviendo 'NA' si es NULL o placeholder (1753-01-01)."""
    if f is None:
        return "NA"
    if isinstance(f, datetime) and f.year == 1753:
        return "NA"
    return str(f)[:10]  # yyyy-mm-dd

def main():
    parser = argparse.ArgumentParser(description='Consultar fechas de obra desde Navision')
    parser.add_argument('--id', type=str, default='880', help='ID de obra (por defecto: 880)')
    
    args = parser.parse_args()
    
    print(f"🔍 CONSULTA DE FECHAS PARA OBRA ID: {args.id}")
    print("=" * 60)
    print("📊 Base de datos: Navision (SQL Server)")
    
    try:
        fechas = get_fechas_obra(args.id)
        
        if fechas:
            print(f"\n📅 Fechas de la obra {args.id}:")
            print(f"   📋 Recepción definitiva: {_fmt_fecha(fechas.recepcion)}")
            print(f"   🏆 Adjudicación: {_fmt_fecha(fechas.adjudicacion)}")
            print(f"   ✍️ Firma contrato: {_fmt_fecha(fechas.firma_contrato)}")
            print(f"   📐 Acta replanteo: {_fmt_fecha(fechas.replanteo)}")
            print(f"   🏁 Fin contrato: {_fmt_fecha(fechas.fin_contrato)}")
            print(f"\n✅ Datos encontrados para la obra {args.id}")
        else:
            print(f"⚠️ No se encontraron datos para la obra {args.id}")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("💡 Verifica las credenciales de Navision en el archivo .env")

if __name__ == "__main__":
    main()
