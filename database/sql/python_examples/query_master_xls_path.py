import os
import argparse
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

load_dotenv()

conninfo = (
    f"host={os.getenv('PGHOST')} "
    f"port={os.getenv('PGPORT')} "
    f"dbname={os.getenv('PGDATABASE')} "
    f"user={os.getenv('PGUSER')} "
    f"password={os.getenv('PGPASSWORD')} "
    f"sslmode={os.getenv('PGSSLMODE', 'prefer')}"
)

def main():
    parser = argparse.ArgumentParser(description='Consultar ruta de archivo Excel por ID de obra')
    parser.add_argument('--id', type=str, default='880', help='ID de obra (por defecto: 880)')
    
    args = parser.parse_args()
    
    print(f"🔍 RUTA PARA OBRA ID: {args.id}")
    print("=" * 50)
    
    SQL = "SELECT codigo_obra, path FROM public.masters_masterficha WHERE codigo_obra = %s;"
    
    with psycopg.connect(conninfo) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(SQL, (args.id,))
            rows = cur.fetchall()
            
            if not rows:
                print(f"❌ No se encontró ninguna ruta para la obra ID: {args.id}")
                return
            
            for row in rows:
                codigo_obra = row['codigo_obra']
                path = row['path']
                print(f"✅ ID: {codigo_obra} | Path: {path}")

if __name__ == "__main__":
    main()