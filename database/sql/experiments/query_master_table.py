import os
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

def get_table_sample_data(table_name, limit=2):
    """Obtiene una muestra de datos de una tabla específica."""
    SQL = f"SELECT * FROM public.{table_name} LIMIT %s;"
    
    with psycopg.connect(conninfo) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(SQL, (limit,))
            return cur.fetchall()

def get_all_ids():
    """Obtiene todos los IDs disponibles en la tabla."""
    SQL = "SELECT codigo_obra FROM public.masters_masterficha ORDER BY codigo_obra;"
    
    with psycopg.connect(conninfo) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(SQL)
            return cur.fetchall()

def main():
    print("🗄️ MUESTRA DE DATOS - MASTERS_MASTERFICHA")
    print("=" * 60)
    
    table_name = 'masters_masterficha'
    print(f"\n📊 TABLA: {table_name}")
    print("-" * 50)
    
    # Mostrar todos los IDs disponibles
    print("\n🔢 TODOS LOS IDs DISPONIBLES:")
    print("-" * 50)
    try:
        all_ids = get_all_ids()
        if all_ids:
            print(f"📈 Total de IDs encontrados: {len(all_ids)}")
            print("-" * 50)
            
            # Mostrar IDs en columnas de 10
            for i, row in enumerate(all_ids, 1):
                codigo_obra = row['codigo_obra']
                print(f"{codigo_obra:>6}", end="  ")
                if i % 10 == 0:  # Nueva línea cada 10 IDs
                    print()
            if len(all_ids) % 10 != 0:  # Nueva línea final si no es múltiplo de 10
                print()
        else:
            print("❌ No se encontraron IDs")
    except Exception as e:
        print(f"❌ Error al obtener IDs: {str(e)}")
    
    # Mostrar muestra de datos
    print(f"\n📋 MUESTRA DE DATOS (2 filas):")
    print("-" * 50)
    try:
        # Obtener muestra de datos
        sample_data = get_table_sample_data(table_name, 2)
        
        if not sample_data:
            print("❌ No se encontraron datos")
            return
        
        print(f"📈 Mostrando {len(sample_data)} filas de muestra:")
        print("-" * 50)
        
        # Mostrar datos
        for i, row in enumerate(sample_data, 1):
            print(f"Fila {i}:")
            for key, value in row.items():
                # Truncar valores muy largos
                if isinstance(value, str) and len(str(value)) > 50:
                    value = str(value)[:47] + "..."
                print(f"  {key}: {value}")
            print("-" * 30)
            
    except Exception as e:
        print(f"❌ Error al obtener datos de {table_name}: {str(e)}")
    
    print(f"\n🎉 Exploración completada")

if __name__ == "__main__":
    main()
