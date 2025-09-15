# query_prediccion_cache.py
import os
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row  # opcional: filas como dict

load_dotenv()

conninfo = (
    f"host={os.getenv('PGHOST')} "
    f"port={os.getenv('PGPORT')} "
    f"dbname={os.getenv('PGDATABASE')} "
    f"user={os.getenv('PGUSER')} "
    f"password={os.getenv('PGPASSWORD')} "
    f"sslmode={os.getenv('PGSSLMODE', 'prefer')}"
)

SQL = "SELECT * FROM public.prediccion_cache ORDER BY data_key ASC LIMIT 10;"

def main():
    with psycopg.connect(conninfo) as conn:
        # si prefieres tuplas, quita row_factory=dict_row
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(SQL)
            rows = cur.fetchall()
            for row in rows:
                print(row)

if __name__ == "__main__":
    main()
